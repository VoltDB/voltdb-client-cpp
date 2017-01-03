/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include <iostream>
#include "MockVoltDB.h"
#include "Client.h"
#include "ClientImpl.h"
#include <arpa/inet.h>
#include "ByteBuffer.hpp"
#include "ByteBuffer.hpp"
#include <cstdio>
#include <event2/buffer.h>
#include <boost/scoped_ptr.hpp>
#include <boost/scoped_array.hpp>

namespace voltdb {

class CxnContext {
/*
 * Data associated with a specific connection
 */
public:
    CxnContext(std::string name) : m_name(name), m_nextLength(4), m_lengthOrMessage(true), m_authenticated(false) {

    }
    const std::string m_name;
    int32_t m_nextLength;
    bool m_lengthOrMessage;
    bool m_authenticated;
};

SharedByteBuffer fileAsByteBuffer(std::string filename) {

    int ret = 0;

    std::string path = "test_src/test_data/" + filename;
    FILE *fp = fopen(path.c_str(), "r");
    assert(fp);
    ret = fseek(fp, 0L, SEEK_END);
    assert(!ret);
    size_t size = ftell(fp);
    ret = fseek(fp, 0L, SEEK_SET);
    assert(!ret);
    assert(ftell(fp) == 0);

    char *buffer = new char[size];
    SharedByteBuffer b(buffer, (int)size);

    size_t bytes_read = fread(buffer, 1, size, fp);
    if (bytes_read != size) {
        printf("Failed to read file at %s with size %d (read: %d)\n",
            path.c_str(), (int)size, (int)bytes_read);
    }
    assert(bytes_read == size);

    fclose(fp);

    return b;
}

/**
   type definition for the read or write callback.

   The read callback is triggered when new data arrives in the input
   buffer and the amount of readable data exceed the low watermark
   which is 0 by default.

   The write callback is triggered if the write buffer has been
   exhausted or fell below its low watermark.

   @param bev the bufferevent that triggered the callback
   @param ctx the user specified context for this bufferevent
 */
static void readCallback(struct bufferevent *bev, void *ctx) {
    MockVoltDB *impl = reinterpret_cast<MockVoltDB*>(ctx);
    impl->readCallback(bev);
}

/*
 * Only has to handle the case where there is an error or EOF
 */
static void eventCallback(struct bufferevent *bev, short events, void *ctx) {
    MockVoltDB *impl = reinterpret_cast<MockVoltDB*>(ctx);
    impl->eventCallback(bev, events);
}

/**
   type definition for the read or write callback.

   The read callback is triggered when new data arrives in the input
   buffer and the amount of readable data exceed the low watermark
   which is 0 by default.

   The write callback is triggered if the write buffer has been
   exhausted or fell below its low watermark.

   @param bev the bufferevent that triggered the callback
   @param ctx the user specified context for this bufferevent
 */
static void writeCallback(struct bufferevent *bev, void *ctx) {
    MockVoltDB *impl = reinterpret_cast<MockVoltDB*>(ctx);
    impl->writeCallback(bev);
}

static void acceptCallback(struct evconnlistener *listener,
    evutil_socket_t fd, struct sockaddr *address, int socklen,
    void *ctx) {
    MockVoltDB *impl = reinterpret_cast<MockVoltDB*>(ctx);
    impl->acceptCallback(listener, fd, address, socklen);
}


MockVoltDB::MockVoltDB(Client client) : m_base(client.m_impl->m_base), m_listener(NULL),
        m_hangupOnRequestCounter(-1), m_dontRead(false), m_timeoutCount(-1), m_errorCount(-1), m_client(client) {
    struct sockaddr_in sin;
    ::memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = htonl(0);
    sin.sin_port = htons(21212);

    m_listener =
            evconnlistener_new_bind(
                    m_base,
                    voltdb::acceptCallback,
                    this,
                    LEV_OPT_CLOSE_ON_FREE | LEV_OPT_REUSEABLE,
                    -1,
                    (struct sockaddr*)&sin,
                    sizeof(sin));
    if (!m_listener) {
        throw LibEventException();
    }
}

static void interrupt_callback(evutil_socket_t fd, short events, void *clientData) {
    MockVoltDB *self = reinterpret_cast<MockVoltDB*>(clientData);
    self->eventBaseLoopBreak();
}

void MockVoltDB::eventBaseLoopBreak() {
    event_base_loopbreak(m_base);
}

void MockVoltDB::interrupt() {
    event_base_once(m_base, -1, EV_TIMEOUT, interrupt_callback, this, NULL);
}

void MockVoltDB::run() 
throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::LibEventException) {
    if (event_base_dispatch(m_base) == -1) {
        throw voltdb::LibEventException();
    }
}

MockVoltDB::~MockVoltDB() {
    for (std::set<struct bufferevent*>::iterator i = m_connections.begin(); i != m_connections.end(); i++) {
        bufferevent_free(*i);
    }
    m_connections.clear();
    if (m_listener != NULL) {
        evconnlistener_free(m_listener);
    }
}

void MockVoltDB::mimicLargeReply(int64_t clientData, struct bufferevent *bev) {
    char *storage = new char[32];
    SharedByteBuffer *response = new SharedByteBuffer(storage, 32);
    SharedByteBuffer tabledata = fileAsByteBuffer("large_serialized_table.bin");

    // write a valid message header (length and protoVersion)
    int32_t ttl_length = 18 + (51380323);
    response->ensureCapacity(ttl_length);
    response->putInt32(ttl_length); // msg length prefix (not self-inclusive)
    response->putInt8(0); // protocol version

    // write a valid invocation response header.
    response->putInt64(clientData); // handle
    response->putInt8(0);  // opt-out of "optional fields"
    response->putInt8(1);  // status code:
    response->putInt8(0);  // appstatus code
    response->putInt32(1); // client round trip time!
    response->putInt16(1); // result count.

    // write a table, using the serialized table on disk.
    // response->putInt32(51380323); // table size (via ls).
    response->put(&tabledata);

    response->flip();
    struct evbuffer *evbuf = bufferevent_get_output(bev);
    if (evbuffer_add(evbuf, response->bytes(), static_cast<size_t>(response->remaining()))) {
        throw voltdb::LibEventException();
    }

    delete response;
}

void MockVoltDB::readCallback(struct bufferevent *bev) {
    if (m_dontRead) {
        return;
    }


    if (m_hangupOnRequestCounter == 0) {
        bufferevent_free(bev);
        m_contexts.erase(bev);
        m_connections.erase(bev);
        return;
    }

    boost::shared_ptr<CxnContext> ctx = m_contexts[bev];
    if (!ctx->m_authenticated) {
        if (m_hangupOnRequestCounter > 0) {
            m_hangupOnRequestCounter--;
            if (m_hangupOnRequestCounter == 0) {
                bufferevent_free(bev);
                m_contexts.erase(bev);
                m_connections.erase(bev);
                return;
            }
        }
        SharedByteBuffer response = fileAsByteBuffer("authentication_response.msg");
        struct evbuffer *evbuf = bufferevent_get_output(bev);
        if (evbuffer_add(evbuf, response.bytes(), static_cast<size_t>(response.remaining()))) {
            throw voltdb::LibEventException();
        }
        ctx->m_authenticated = true;

        evbuf = bufferevent_get_input(bev);
        char lengthBytes[4];
        evbuffer_remove(evbuf, lengthBytes, 4);
        ByteBuffer lengthBuffer(lengthBytes, 4);
        int32_t length = lengthBuffer.getInt32();
        boost::scoped_array<char> message(new char[length]);
        evbuffer_remove(evbuf, message.get(), length );
        return;
    }

    struct evbuffer *evbuf = bufferevent_get_input(bev);
    while (evbuffer_get_length(evbuf) > 0)  {
        if (m_hangupOnRequestCounter > 0) {
            m_hangupOnRequestCounter--;
            if (m_hangupOnRequestCounter == 0) {
                bufferevent_flush(bev, EV_WRITE, BEV_FINISHED);
                bufferevent_disable(bev, EV_READ);
                bufferevent_enable(bev, EV_WRITE);
                bufferevent_setwatermark( bev, EV_WRITE, 0, 262144);
                return;
            }
        }
        if (m_filenameForNextResponse == "") {
            throw std::exception();
        }
        // message length
        char lengthBytes[4];
        evbuffer_remove(evbuf, lengthBytes, 4);
        ByteBuffer lengthBuffer(lengthBytes, 4);
        int32_t length = lengthBuffer.getInt32();
        // message
        boost::scoped_array<char> message(new char[length]);
        evbuffer_remove(evbuf, message.get(), length );
        ByteBuffer messageBuffer(message.get(), length);
        // ??
        messageBuffer.getInt8();
        bool wasNull;
        messageBuffer.getString(wasNull);
        int64_t clientData = messageBuffer.getInt64();

        if (m_filenameForNextResponse == "mimicLargeReply") {
            this->mimicLargeReply(clientData, bev);
        }
        else {
            SharedByteBuffer response;
            response = fileAsByteBuffer(m_filenameForNextResponse);
            //some blunt changes to allow a mix of responses to be sent, to test multi procedure invocations.
            if (m_errorCount == 0) {
                std::cout << "------> MockVoltDB failure response being sent" << std::endl;
                response = fileAsByteBuffer("invocation_response_fail_cv.msg");
                m_errorCount=-1;
            } else if (m_errorCount > 0) {
                std::cout << "------> MockVoltDB failure response after " << m_errorCount << " messages" << std::endl;
                m_errorCount--;
            }
        	//some more blunt changes to allow a single transaction in a multi procedure invocation to timeout.
            if (m_timeoutCount == 0) {
                std::cout << "------> MockVoltDB is timing out this request!!!!!!!!" << std::endl;
                m_timeoutCount=-1;
                bufferevent_flush(bev, EV_WRITE, BEV_FINISHED);
                bufferevent_disable(bev, EV_READ);
                bufferevent_enable(bev, EV_WRITE);
                bufferevent_setwatermark( bev, EV_WRITE, 0, 262144);
                return;
            } else {
                if (m_timeoutCount > 0) {
                    std::cout << "------> MockVoltDB timeout response after " << m_timeoutCount << " messages" << std::endl;
                    m_timeoutCount--;
                }
            }
            
            response.putInt64( 5, clientData);
            evbuf = bufferevent_get_output(bev);
            if (evbuffer_add(evbuf, response.bytes(), static_cast<size_t>(response.remaining()))) {
                throw voltdb::LibEventException();
            }
        }
        evbuf = bufferevent_get_input(bev);
    }
}

void MockVoltDB::eventCallback(struct bufferevent *bev, short events) {}
void MockVoltDB::writeCallback(struct bufferevent *bev) {
    if (m_hangupOnRequestCounter == 0) {
        bufferevent_free(bev);
        m_contexts.erase(bev);
        m_connections.erase(bev);
    }
}
void MockVoltDB::acceptCallback(
        struct evconnlistener *listener,
        evutil_socket_t sock,
        struct sockaddr *addr,
        int len) {
    struct bufferevent *bev = bufferevent_socket_new( m_base, sock, BEV_OPT_CLOSE_ON_FREE);
    bufferevent_setcb( bev, voltdb::readCallback, voltdb::writeCallback, voltdb::eventCallback, this);
    bufferevent_setwatermark( bev, EV_READ, 0, 256);
    bufferevent_enable(bev, EV_READ);
    m_connections.insert(bev);
    m_contexts[bev] = boost::shared_ptr<CxnContext>(new CxnContext(""));
}

}

