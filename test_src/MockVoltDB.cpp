/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

#include "MockVoltDB.h"
#include "Client.h"
#include "ClientImpl.h"
#include <arpa/inet.h>
#include "ByteBuffer.hpp"
#include "ByteBuffer.hpp"
#include <event2/buffer.h>
#include <boost/scoped_ptr.hpp>
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
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
    boost::filesystem::path p( "../test/" + filename);
    if (!boost::filesystem::exists(p)) {
        p = boost::filesystem::path( "test/" + filename);
        if (!boost::filesystem::exists(p)) {
            throw std::exception();
        }
    }

    size_t size = boost::filesystem::file_size(p);
    char *buffer = new char[size];
    SharedByteBuffer b(buffer, size);
    boost::filesystem::ifstream ifs(p);
    ifs.read(buffer, size);
    if (ifs.fail()) {
        throw std::exception();
    }
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
        m_hangupOnRequestCounter(-1), m_dontRead(false), m_client(client)  {
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

MockVoltDB::~MockVoltDB() {
    for (boost::unordered_set<struct bufferevent*>::iterator i = m_connections.begin(); i != m_connections.end(); i++) {
        bufferevent_free(*i);
    }
    m_connections.clear();
    if (m_listener != NULL) {
        evconnlistener_free(m_listener);
    }
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
        char lengthBytes[4];
        evbuffer_remove(evbuf, lengthBytes, 4);
        ByteBuffer lengthBuffer(lengthBytes, 4);
        int32_t length = lengthBuffer.getInt32();
        boost::scoped_array<char> message(new char[length]);
        evbuffer_remove(evbuf, message.get(), length );
        ByteBuffer messageBuffer(message.get(), length);
        messageBuffer.getInt8();
        bool wasNull;
        messageBuffer.getString(wasNull);
        int64_t clientData = messageBuffer.getInt64();

        SharedByteBuffer response = fileAsByteBuffer(m_filenameForNextResponse);
        response.putInt64( 5, clientData);
        evbuf = bufferevent_get_output(bev);
        if (evbuffer_add(evbuf, response.bytes(), static_cast<size_t>(response.remaining()))) {
            throw voltdb::LibEventException();
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

