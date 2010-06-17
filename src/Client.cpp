/*
 * Client.cpp
 *
 *  Created on: Jun 11, 2010
 *      Author: aweisberg
 */
#include "Client.h"
#include <event2/buffer.h>
#include <event2/util.h>
#include <event2/dns.h>
#include <boost/scoped_array.hpp>
#include "AuthenticationRequest.hpp"
#include "AuthenticationResponse.hpp"

namespace voltdb {
class PendingConnection {
public:
    PendingConnection(Client *client, struct event_base *base)
        :  m_status(true), m_client(client), m_base(base), m_authenticationResponseLength(-1) {}
    bool m_status;
    Client *m_client;
    struct event_base *m_base;
    int32_t m_authenticationResponseLength;
    AuthenticationResponse m_response;
    struct timeval post_connect_wait;
};

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
void authenticationReadCallback(struct bufferevent *bev, void *ctx) {
    PendingConnection *pc = reinterpret_cast<PendingConnection*>(ctx);
    struct evbuffer *evbuf = bufferevent_get_input(bev);
    if (pc->m_authenticationResponseLength < 0) {
        char messageLengthBytes[4];
        int read = evbuffer_remove(evbuf, messageLengthBytes, 4);
        assert(read == 4);
        ByteBuffer messageLengthBuffer(messageLengthBytes, 4);
        int32_t messageLength = messageLengthBuffer.getInt32();
        assert(messageLength > 0);
        assert(messageLength < 1024 * 1024);
        pc->m_authenticationResponseLength = messageLength;
        if (evbuffer_get_length(evbuf) < static_cast<size_t>(messageLength)) {
            bufferevent_setwatermark( bev, EV_READ, static_cast<size_t>(messageLength), 262144);
            return;
        }
    }

    ScopedByteBuffer buffer(pc->m_authenticationResponseLength);
    int read = evbuffer_remove(evbuf, buffer.bytes(), static_cast<size_t>(pc->m_authenticationResponseLength));
    assert(read == pc->m_authenticationResponseLength);
    pc->m_response = AuthenticationResponse(buffer);
    pc->post_connect_wait.tv_sec = 0;
    pc->post_connect_wait.tv_usec = 10000;
    event_base_loopexit( pc->m_base, &pc->post_connect_wait);
    bufferevent_setwatermark( bev, EV_READ, 4, 262144);
}

/**
   type definition for the error callback of a bufferevent.

   The error callback is triggered if either an EOF condition or another
   unrecoverable error was encountered.

   @param bev the bufferevent for which the error condition was reached
   @param what a conjunction of flags: BEV_EVENT_READING or BEV_EVENT_WRITING
      to indicate if the error was encountered on the read or write path,
      and one of the following flags: BEV_EVENT_EOF, BEV_EVENT_ERROR,
      BEV_EVENT_TIMEOUT, BEV_EVENT_CONNECTED.

   @param ctx the user specified context for this bufferevent
*/
void authenticationEventCallback(struct bufferevent *bev, short events, void *ctx) {
    PendingConnection *pc = reinterpret_cast<PendingConnection*>(ctx);
    if (events & BEV_EVENT_CONNECTED) {
        printf("connected okay\n");
    } else if (events & (BEV_EVENT_ERROR | BEV_EVENT_EOF)) {
        pc->m_status = false;
        printf("closing\n");
    }

}

void regularEventCallback(struct bufferevent *bev, short events, void *ctx) {
    if (events & BEV_EVENT_CONNECTED) {
        printf("connected okay\n");
    } else if (events & (BEV_EVENT_ERROR | BEV_EVENT_EOF)) {
        printf("closing\n");
    }
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
void regularWriteCallback(struct bufferevent *bev, void *ctx) {
    CxnContext *context = reinterpret_cast<CxnContext*>(ctx);
    boost::unordered_set<struct bufferevent*>::iterator iter =
            context->m_backpressuredBevs->find(bev);
    if (iter != context->m_backpressuredBevs->end()) {
        context->m_backpressuredBevs->erase(iter);
    }
    if (context->m_invocationBlockedOnBackpressure) {
        event_base_loopbreak(context->m_base);
    }
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
void regularReadCallback(struct bufferevent *bev, void *ctx) {
    CxnContext *context = reinterpret_cast<CxnContext*>(ctx);
    struct evbuffer *evbuf = bufferevent_get_input(bev);
    int32_t remaining = static_cast<int32_t>(evbuffer_get_length(evbuf));
    bool breakEventLoop = false;

    if (context->m_lengthOrMessage && remaining < 4) {
        throw std::string("Shouldn't happen");
    }

    while (true) {
        if (context->m_lengthOrMessage && remaining >= 4) {
            char lengthBytes[4];
            ByteBuffer lengthBuffer(lengthBytes, 4);
            evbuffer_remove( evbuf, lengthBytes, 4);
            context->m_nextLength = static_cast<size_t>(lengthBuffer.getInt32());
            context->m_lengthOrMessage = false;
            remaining -= 4;
        } else if (remaining >= context->m_nextLength) {
            boost::shared_array<char> messageBytes = boost::shared_array<char>(new char[context->m_nextLength]);
            context->m_lengthOrMessage = true;
            evbuffer_remove( evbuf, messageBytes.get(), static_cast<size_t>(context->m_nextLength));
            remaining -= context->m_nextLength;
            boost::shared_ptr<InvocationResponse> response(new InvocationResponse(messageBytes, context->m_nextLength));
            CallbackMap *callbackMap = (*context->m_callbacks)[bev];
            CallbackMap::iterator i = callbackMap->find(response->clientData());
            if (i->second->callback(response)) {
                breakEventLoop = true;
            }
            callbackMap->erase(i);
        } else {
            if (context->m_lengthOrMessage) {
                bufferevent_setwatermark( bev, EV_READ, 4, 262144);
            } else {
                bufferevent_setwatermark( bev, EV_READ, static_cast<size_t>(context->m_nextLength), 262144);
            }
            break;
        }
    }

    if (breakEventLoop) {
        event_base_loopbreak( context->m_base );
    }
}

boost::shared_ptr<Client> Client::create() {
    return boost::shared_ptr<Client>(new Client());
}

Client::~Client() {
    for (std::vector<std::pair<struct bufferevent *, boost::shared_ptr<CxnContext> > >::iterator i = m_bevs.begin(); i != m_bevs.end(); i++) {
        bufferevent_free(i->first);
    }
    m_bevs.clear();
    event_base_free(m_base);
    for (BEVToCallbackMap::iterator i = m_callbacks.begin(); i != m_callbacks.end(); i++) {
        delete (*i).second;
    }
}

Client::Client() : m_nextRequestId(0), m_nextConnectionIndex(0) {
    //event_enable_debug_mode();
    m_base = event_base_new();
    assert(m_base);
}

bool Client::createConnection(std::string hostname, std::string username, std::string password) {
    struct bufferevent *bev = bufferevent_socket_new(m_base, -1, BEV_OPT_CLOSE_ON_FREE);
    if (bev == NULL) {
        return false;
    }
    PendingConnection pc(this, m_base);
    bufferevent_setcb(bev, authenticationReadCallback, NULL, authenticationEventCallback, &pc);
    if (bufferevent_socket_connect_hostname(bev, NULL, AF_INET, hostname.c_str(), 21212) != 0) {
        bufferevent_free(bev);
        return false;
    }
    event_base_dispatch(m_base);
    if (pc.m_status) {
        bufferevent_setwatermark( bev, EV_READ, 4, 262144);
        bufferevent_setwatermark( bev, EV_WRITE, 8192, 262144);
        int result = bufferevent_enable(bev, EV_READ);
        if (result != 0) {
            bufferevent_free(bev);
            return false;
        }
        AuthenticationRequest authRequest( username, "database", password );
        ScopedByteBuffer bb(authRequest.getSerializedSize());
        authRequest.serializeTo(bb);
        struct evbuffer *evbuf = bufferevent_get_output(bev);
        result = evbuffer_add( evbuf, bb.bytes(), static_cast<size_t>(bb.remaining()));
        if (result != 0) {
            bufferevent_free(bev);
            return false;
        }
        event_base_dispatch(m_base);
        if (pc.m_status) {
            bufferevent_setwatermark( bev, EV_READ, 4, 262144);
            boost::shared_ptr<CxnContext> cc =
                    boost::shared_ptr<CxnContext>(new CxnContext(this, m_base, &m_backpressuredBevs, &m_callbacks));
            m_bevs.push_back(std::pair<struct bufferevent *, boost::shared_ptr<CxnContext> >( bev, cc));
            m_callbacks[bev] = new CallbackMap();
            bufferevent_setcb(bev, regularReadCallback, regularWriteCallback, regularEventCallback, cc.get());
            return true;
        } else {
            bufferevent_free(bev);
            return false;
        }
    } else {
        return false;
        printf("Connection failure\n");
    }
    return false;
}

class SyncCallback : public ProcedureCallback {
public:
    SyncCallback(boost::shared_ptr<InvocationResponse> *responseOut) : m_responseOut(responseOut) {
    }

    bool callback(boost::shared_ptr<InvocationResponse> response) {
        (*m_responseOut) = response;
        return true;
    }
private:
    boost::shared_ptr<InvocationResponse> *m_responseOut;
};

boost::shared_ptr<InvocationResponse> Client::invoke(Procedure *proc) {
    int32_t messageSize = proc->getSerializedSize();
    ScopedByteBuffer sbb(messageSize);
    int64_t clientData = m_nextRequestId++;
    proc->serializeTo(&sbb, clientData);
    struct bufferevent *bev = m_bevs[m_nextConnectionIndex++ % m_bevs.size()].first;
    boost::shared_ptr<InvocationResponse> response;
    boost::shared_ptr<ProcedureCallback> callback(new SyncCallback(&response));
    struct evbuffer *evbuf = bufferevent_get_output(bev);
    evbuffer_add(evbuf, sbb.bytes(), static_cast<size_t>(sbb.remaining()));
    (*m_callbacks[bev])[clientData] = callback;
    event_base_dispatch(m_base);
    return response;
}

void Client::invoke(Procedure *proc, boost::shared_ptr<ProcedureCallback> callback) {
    int32_t messageSize = proc->getSerializedSize();
    ScopedByteBuffer sbb(messageSize);
    int64_t clientData = m_nextRequestId++;
    proc->serializeTo(&sbb, clientData);

    struct bufferevent *bev = NULL;
    while (true) {
        for (size_t ii = 0; ii < m_bevs.size(); ii++) {
            bev = m_bevs[ii].first;
            if (m_backpressuredBevs.find(bev) != m_backpressuredBevs.end()) {
                bev = NULL;
            } else {
                break;
            }
        }
        if (bev) {
            break;
        }
        event_base_dispatch(m_base);
    }

    struct evbuffer *evbuf = bufferevent_get_output(bev);
    evbuffer_add(evbuf, sbb.bytes(), static_cast<size_t>(sbb.remaining()));
    (*m_callbacks[bev])[clientData] = callback;

    if (evbuffer_get_length(evbuf) >  262144) {
        m_backpressuredBevs.insert(bev);
    }

    return;
}

bool Client::runOnce() {
    if (event_base_loop(m_base, EVLOOP_NONBLOCK) != 0) {
        return false;
    }
    return true;
}

bool Client::run() {
    if (event_base_dispatch(m_base) != 0) {
        return false;
    }
    return true;
}
}
