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
        :  m_status(true), m_client(client), m_base(base), m_authenticationResponseLength(-1),
           m_loginExchangeCompleted(false) {}
    bool m_status;
    Client *m_client;
    struct event_base *m_base;
    int32_t m_authenticationResponseLength;
    AuthenticationResponse m_response;
    struct timeval m_post_connect_wait;
    bool m_loginExchangeCompleted;
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
    pc->m_post_connect_wait.tv_sec = 0;
    pc->m_post_connect_wait.tv_usec = 10000;
    pc->m_loginExchangeCompleted = true;
    event_base_loopexit( pc->m_base, &pc->m_post_connect_wait);
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
    } else if (events & (BEV_EVENT_ERROR | BEV_EVENT_EOF)) {
        pc->m_status = false;
        pc->m_loginExchangeCompleted = true;
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
            boost::shared_ptr<CallbackMap> callbackMap = (*context->m_callbacks)[bev];
            CallbackMap::iterator i = callbackMap->find(response->clientData());
            try {
                if (i->second->callback(response)) {
                    breakEventLoop = true;
                }
            } catch (std::exception e) {
                if (context->m_listener.get() != NULL) {
                    context->m_listener->uncaughtException( e, i->second);
                }
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

/*
 * Only has to handle the case where there is an error or EOF
 */
void regularEventCallback(struct bufferevent *bev, short events, void *ctx) {
    if (events & BEV_EVENT_CONNECTED) {
        assert(false);
    } else if (events & (BEV_EVENT_ERROR | BEV_EVENT_EOF)) {
        /*
         * First drain anything in the read buffer
         */
        regularReadCallback(bev, ctx);
        CxnContext *context = reinterpret_cast<CxnContext*>(ctx);

        //Notify client that a connection was lost
        if (context->m_listener.get() != NULL) {
            context->m_listener->connectionLost(context->m_hostname, context->m_bevs->size() - 1);
        }

        /*
         * Iterate the list of callbacks for this connection and invoke them
         * with the appropriate error response
         */
        BEVToCallbackMap::iterator callbackMapIter = context->m_callbacks->find(bev);
        boost::shared_ptr<CallbackMap> callbackMap = callbackMapIter->second;
        bool breakEventLoop = false;
        for (CallbackMap::iterator i =  callbackMap->begin();
                i != callbackMap->end(); i++) {
            try {
                breakEventLoop =
                        i->second->callback(boost::shared_ptr<voltdb::InvocationResponse>(new InvocationResponse()));
            } catch (std::exception e) {
                if (context->m_listener.get() != NULL) {
                    context->m_listener->uncaughtException( e, i->second);
                }
            }
        }

        if (breakEventLoop) {
            event_base_loopbreak( context->m_base );
        }

        //Remove the callback map from the main connection map
        context->m_callbacks->erase(callbackMapIter);

        //remove the entry for the backpressured connection
        context->m_backpressuredBevs->erase(bev);

        //Remove the connection from the list of connections, deleting the shared pointer to the context by extension
        for (std::vector<std::pair<struct bufferevent *, boost::shared_ptr<CxnContext> > >::iterator i =
                context->m_bevs->begin();
                i != context->m_bevs->end(); i++) {
            if (i->first == bev) {
                context->m_bevs->erase(i);
                break;
            }
        }
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
    if (*context->m_invocationBlockedOnBackpressure) {
        *context->m_invocationBlockedOnBackpressure = false;
        event_base_loopbreak(context->m_base);
    }
}

boost::shared_ptr<Client> Client::create() throw(voltdb::LibEventException) {
    return boost::shared_ptr<Client>(new Client());
}
boost::shared_ptr<Client> Client::create(boost::shared_ptr<voltdb::StatusListener> listener) throw(voltdb::LibEventException) {
    return boost::shared_ptr<Client>(new Client(listener));
}

Client::~Client() {
    for (std::vector<std::pair<struct bufferevent *, boost::shared_ptr<CxnContext> > >::iterator i = m_bevs.begin(); i != m_bevs.end(); i++) {
        bufferevent_free(i->first);
    }
    m_bevs.clear();
    event_base_free(m_base);
}

Client::Client() throw(voltdb::LibEventException) :
        m_nextRequestId(0), m_nextConnectionIndex(0),
        m_invocationBlockedOnBackpressure(false), m_loopBreakRequested(false) {
#ifdef DEBUG
    event_enable_debug_mode();
#endif
    m_base = event_base_new();
    assert(m_base);
    if (!m_base) {
        throw voltdb::LibEventException();
    }
}

Client::Client(boost::shared_ptr<voltdb::StatusListener> listener) throw(voltdb::LibEventException) :
        m_nextRequestId(0), m_nextConnectionIndex(0), m_listener(listener),
        m_invocationBlockedOnBackpressure(false), m_loopBreakRequested(false) {
#ifdef DEBUG
    event_enable_debug_mode();
#endif
    m_base = event_base_new();
    assert(m_base);
    if (!m_base) {
        throw voltdb::LibEventException();
    }
}

class FreeBEVOnFailure {
public:
    FreeBEVOnFailure(struct bufferevent *bev) : m_bev(bev), m_success(false) {

    }
    ~FreeBEVOnFailure() {
        if (!m_success) {
            bufferevent_free(m_bev);
        }
    }
    void success() {
        m_success = true;
    }
private:
    struct bufferevent *m_bev;
    bool m_success;
};

void Client::createConnection(std::string hostname, std::string username, std::string password) throw (voltdb::Exception, voltdb::ConnectException, voltdb::LibEventException) {
    struct bufferevent *bev = bufferevent_socket_new(m_base, -1, BEV_OPT_CLOSE_ON_FREE);
    FreeBEVOnFailure protector(bev);
    if (bev == NULL) {
        throw ConnectException();
    }
    PendingConnection pc(this, m_base);
    bufferevent_setcb(bev, authenticationReadCallback, NULL, authenticationEventCallback, &pc);
    if (bufferevent_socket_connect_hostname(bev, NULL, AF_INET, hostname.c_str(), 21212) != 0) {
        throw voltdb::LibEventException();
    }
    if (event_base_dispatch(m_base) == -1) {
        throw voltdb::LibEventException();
    }
    if (pc.m_status) {
        bufferevent_setwatermark( bev, EV_READ, 4, 262144);
        bufferevent_setwatermark( bev, EV_WRITE, 8192, 262144);
        if (bufferevent_enable(bev, EV_READ)) {
            throw voltdb::LibEventException();
        }
        AuthenticationRequest authRequest( username, "database", password );
        ScopedByteBuffer bb(authRequest.getSerializedSize());
        authRequest.serializeTo(bb);
        struct evbuffer *evbuf = bufferevent_get_output(bev);
        if (evbuffer_add( evbuf, bb.bytes(), static_cast<size_t>(bb.remaining()))) {
            throw voltdb::LibEventException();
        }
        if (event_base_dispatch(m_base) == -1) {
            throw voltdb::LibEventException();
        }
        if (pc.m_status) {
            bufferevent_setwatermark( bev, EV_READ, 4, 262144);
            boost::shared_ptr<CxnContext> cc =
                    boost::shared_ptr<CxnContext>(
                            new CxnContext(
                                    this,
                                    m_base,
                                    &m_backpressuredBevs,
                                    &m_callbacks,
                                    &m_invocationBlockedOnBackpressure,
                                    m_listener,
                                    &m_bevs,
                                    hostname));
            m_bevs.push_back(std::pair<struct bufferevent *, boost::shared_ptr<CxnContext> >( bev, cc));
            m_callbacks[bev] = boost::shared_ptr<CallbackMap>(new CallbackMap());
            bufferevent_setcb(bev, regularReadCallback, regularWriteCallback, regularEventCallback, cc.get());
            protector.success();
            return;
        } else {
            throw ConnectException();
        }
    } else {
        throw ConnectException();
    }
    throw ConnectException();
}

/*
 * A synchronous callback returns the invocation response to the provided address
 * and requests the event loop break
 */
class SyncCallback : public ProcedureCallback {
public:
    SyncCallback(boost::shared_ptr<InvocationResponse> *responseOut) : m_responseOut(responseOut) {
    }

    bool callback(boost::shared_ptr<InvocationResponse> response) throw (voltdb::Exception) {
        (*m_responseOut) = response;
        return true;
    }
private:
    boost::shared_ptr<InvocationResponse> *m_responseOut;
};

boost::shared_ptr<InvocationResponse> Client::invoke(Procedure &proc) throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::UninitializedParamsException, voltdb::LibEventException) {
    if (m_bevs.empty()) {
        throw voltdb::NoConnectionsException();
    }
    int32_t messageSize = proc.getSerializedSize();
    ScopedByteBuffer sbb(messageSize);
    int64_t clientData = m_nextRequestId++;
    proc.serializeTo(&sbb, clientData);
    struct bufferevent *bev = m_bevs[m_nextConnectionIndex++ % m_bevs.size()].first;
    boost::shared_ptr<InvocationResponse> response;
    boost::shared_ptr<ProcedureCallback> callback(new SyncCallback(&response));
    struct evbuffer *evbuf = bufferevent_get_output(bev);
    if (evbuffer_add(evbuf, sbb.bytes(), static_cast<size_t>(sbb.remaining()))) {
        throw voltdb::LibEventException();
    }
    (*m_callbacks[bev])[clientData] = callback;
    if (event_base_dispatch(m_base) == -1) {
        throw voltdb::LibEventException();
    }
    m_loopBreakRequested = false;
    return response;
}

void Client::invoke(Procedure &proc, boost::shared_ptr<ProcedureCallback> callback) throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::UninitializedParamsException, voltdb::LibEventException) {
    if (m_bevs.empty()) {
        throw voltdb::NoConnectionsException();
    }
    int32_t messageSize = proc.getSerializedSize();
    ScopedByteBuffer sbb(messageSize);
    int64_t clientData = m_nextRequestId++;
    proc.serializeTo(&sbb, clientData);

    /*
     * Decide what connection to buffer the event on.
     * First each connection is checked for backpressure. If there is a connection
     * with no backpressure break.
     *
     *  If none can be found, notify the client application and let it decide whether to queue anyways
     *  or run the event loop until there is no more backpressure.
     *
     *  If queuing anyways just pick the next connection.
     *
     *  If waiting for no more backpressure, then set the m_invocationBlockedOnBackpressure flag
     *  so that the write callback knows to break the event loop once there is a connection with no backpressure and
     *  then enter the event loop.
     *  It is possible for the event loop to break early while there is still backpressure because an unrelated callback
     *  may have been invoked and that unrelated callback may have requested that the event loop be broken. In that
     *  case just queue the request to the next connection despite backpressure so that loop break occurs as requested.
     *  Also set the m_invocationBlockedOnBackpressure flag back to false so that the write callback won't spuriously
     *  break the event loop later.
     */
    struct bufferevent *bev = NULL;
    while (true) {
        for (size_t ii = 0; ii < m_bevs.size(); ii++) {
            bev = m_bevs[++m_nextConnectionIndex % m_bevs.size()].first;
            if (m_backpressuredBevs.find(bev) != m_backpressuredBevs.end()) {
                bev = NULL;
            } else {
                break;
            }
        }
        if (bev) {
            break;
        } else {
            bool callEventLoop = true;
            if (m_listener.get() != NULL) {
                callEventLoop = !m_listener->backpressure(true);
            }
            if (callEventLoop) {
                m_invocationBlockedOnBackpressure = true;
                if (event_base_dispatch(m_base) == -1) {
                    throw voltdb::LibEventException();
                }
                if (m_loopBreakRequested) {
                    m_loopBreakRequested = false;
                    m_invocationBlockedOnBackpressure = false;
                    bev = m_bevs[++m_nextConnectionIndex % m_bevs.size()].first;
                }
            } else {
                bev = m_bevs[++m_nextConnectionIndex % m_bevs.size()].first;
            }
        }
    }

    struct evbuffer *evbuf = bufferevent_get_output(bev);
    if (evbuffer_add(evbuf, sbb.bytes(), static_cast<size_t>(sbb.remaining()))) {
        throw voltdb::LibEventException();
    }
    (*m_callbacks[bev])[clientData] = callback;

    if (evbuffer_get_length(evbuf) >  262144) {
        m_backpressuredBevs.insert(bev);
    }

    return;
}

void Client::runOnce() throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::LibEventException) {
    if (m_bevs.empty()) {
        throw voltdb::NoConnectionsException();
    }
    if (event_base_loop(m_base, EVLOOP_NONBLOCK) == -1) {
        throw voltdb::LibEventException();
    }
    m_loopBreakRequested = false;
}

void Client::run() throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::LibEventException) {
    if (m_bevs.empty()) {
        throw voltdb::NoConnectionsException();
    }
    if (event_base_dispatch(m_base) == -1) {
        throw voltdb::LibEventException();
    }
    m_loopBreakRequested = false;
}
}
