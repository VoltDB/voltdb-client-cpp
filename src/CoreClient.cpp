/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

#include <cassert>
#include <pthread.h>
#include <event2/buffer.h>
#include <event2/thread.h>
#include "sha1.h"
#include "CoreClient.h"
#include "AuthenticationResponse.hpp"
#include "AuthenticationRequest.hpp"

#define HIGH_WATERMARK 1024 * 1024 * 55

namespace voltdb {

struct CallbackPair {
    voltdb_proc_callback callback;
    void *payload;
};

class PreparedInvocation {
public:
    PreparedInvocation()
    : data(NULL), data_size(0), payload(NULL) {}
    PreparedInvocation(char *buf_data, int buf_size, voltdb_proc_callback callback, void *payload)
    : data(buf_data), data_size(buf_size), callback(callback), payload(payload) {}
    boost::shared_array<char> data;
    int data_size;
    voltdb_proc_callback callback;
    void *payload;
};

/*
 * Data associated with a specific connection
 */
class CxnContext {
public:
    CxnContext(struct bufferevent* bev, const voltdb_connection_callback callback)
      : bev(bev),
        client(NULL),
        port(-1),
        backpressure(false),
        loginExchangeCompleted(false),
        connected(false),
        authenticated(false),
        base(NULL),
        connCallback(callback),
        nextLength(-1),
        lengthOrMessage(true),
        outstanding(0) {}

    ~CxnContext() {
        if (bev) bufferevent_free(bev);
        bev = NULL;
    }

    struct bufferevent *bev;
    CoreClient *client;

    // connection id
    std::string hostname;
    int port;

    // status
    bool backpressure;
    bool loginExchangeCompleted;
    bool connected;
    bool authenticated;

    // copied from client
    event_base *base;
    const voltdb_connection_callback connCallback;

    // send / recv
    int32_t nextLength;
    bool lengthOrMessage;

    // request stuff
    int32_t outstanding;
    std::map<int64_t, CallbackPair> callbacks;
};

// forward declaration for libevent init function
void initLibevent();

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
static void regularReadCallback(struct bufferevent *bev, void *ctx) {
    CxnContext *context = reinterpret_cast<CxnContext*>(ctx);
    context->client->regularReadCallback(context);
}

/*
 * Only has to handle the case where there is an error or EOF
 */
static void regularEventCallback(struct bufferevent *bev, short events, void *ctx) {
    CxnContext *context = reinterpret_cast<CxnContext*>(ctx);
    context->client->regularEventCallback(context, events);
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
static void regularWriteCallback(struct bufferevent *bev, void *ctx) {
    CxnContext *context = reinterpret_cast<CxnContext*>(ctx);
    context->client->regularWriteCallback(context);
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
static void authenticationReadCallback(struct bufferevent *bev, void *ctx) {
    CxnContext *context = reinterpret_cast<CxnContext*>(ctx);
    struct evbuffer *evbuf = bufferevent_get_input(bev);
    if (context->nextLength < 0) {
        char messageLengthBytes[4];
        int read = evbuffer_remove(evbuf, messageLengthBytes, 4);
        assert(read == 4);
        ByteBuffer messageLengthBuffer(messageLengthBytes, 4);
        int32_t messageLength = messageLengthBuffer.getInt32();
        assert(messageLength > 0);
        assert(messageLength < 1024 * 1024);
        context->nextLength = messageLength;
        if (evbuffer_get_length(evbuf) < static_cast<size_t>(messageLength)) {
            bufferevent_setwatermark( bev, EV_READ, static_cast<size_t>(messageLength), HIGH_WATERMARK);
            return;
        }
    }

    ScopedByteBuffer buffer(context->nextLength);
    int read = evbuffer_remove(evbuf, buffer.bytes(), static_cast<size_t>(context->nextLength));
    assert(read == context->nextLength);
    AuthenticationResponse response(buffer);

    connection_event event;
    event.hostname = context->hostname;
    event.port = context->port;

    if (context->client->processAuthenticationResponse(context, response)) {
        context->authenticated = true;
        event.type = CONNECTED;
        event.info = "Authenticated and connected to VoltDB Node";
    }
    else {
        event.type = CONNECTION_LOST;
        event.info = "Failed to authenticate or handshake to VoltDB Node";
    }

    context->connCallback(context->client, event);

    context->loginExchangeCompleted = true;
    bufferevent_setwatermark( bev, EV_READ, 4, HIGH_WATERMARK);

    bufferevent_setcb(context->bev,
                      regularReadCallback,
                      regularWriteCallback,
                      regularEventCallback, context);
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
static void authenticationEventCallback(struct bufferevent *bev, short events, void *ctx) {
    CxnContext *context = reinterpret_cast<CxnContext*>(ctx);
    if (events & BEV_EVENT_CONNECTED) {
        context->connected = true;
        context->client->completeAuthenticationRequest(context);
    }
    else if (events & (BEV_EVENT_ERROR | BEV_EVENT_EOF)) {
        context->loginExchangeCompleted = true;

        connection_event event;
        event.hostname = context->hostname;
        event.port = context->port;
        event.type = CONNECTION_LOST;
        event.info = "Failed to establish TCP/IP connection to VoltDB";
        context->connCallback(context->client, event);
    }
}

static void authenticationRequestExtCallback(evutil_socket_t fd, short what, void *ctx) {
    CxnContext *context = reinterpret_cast<CxnContext*>(ctx);
    bufferevent_setcb(context->bev, authenticationReadCallback, NULL, authenticationEventCallback, context);
    if (bufferevent_socket_connect_hostname(context->bev, NULL, AF_INET, context->hostname.c_str(), context->port) != 0) {
        throw voltdb::LibEventException();
    }
}

static void invocationRequestExtCallback(evutil_socket_t fd, short what, void *ctx) {
    CoreClient *impl = reinterpret_cast<CoreClient*>(ctx);
    impl->invocationRequestCallback();
}

// Initialization for the library that only gets called once
pthread_once_t once_initLibevent = PTHREAD_ONCE_INIT;
void initLibevent() {
    // try to run threadsafe libevent
    if (evthread_use_pthreads()) {
        throw voltdb::LibEventException();
    }
}

CoreClient::CoreClient(voltdb_connection_callback callback,
               std::string username,
               std::string password)
 :
        m_nextRequestId(INT64_MIN),
        m_connCallback(callback),
        m_instanceIdIsSet(false),
        m_outstandingRequests(0),
        m_username(username),
        m_data(NULL)
{
    // put libevent init calls here, so they only get called once
    pthread_once(&once_initLibevent, initLibevent);

    m_base = event_base_new();
    assert(m_base);
    if (!m_base) {
        throw voltdb::LibEventException();
    }

    SHA1_CTX context;
    SHA1_Init(&context);
    SHA1_Update( &context, reinterpret_cast<const unsigned char*>(password.data()), password.size());
    SHA1_Final ( &context, m_passwordHash);

    pthread_mutex_init (&m_contexts_mutex, NULL);
    pthread_mutex_init (&m_requests_mutex, NULL);
}

CoreClient::~CoreClient() {
    pthread_mutex_lock(&m_contexts_mutex);
    {
        m_contexts.clear();
    }
    pthread_mutex_unlock(&m_contexts_mutex);
    pthread_mutex_destroy (&m_contexts_mutex);

    pthread_mutex_lock(&m_requests_mutex);
    {
        m_requests.clear();
    }
    pthread_mutex_unlock(&m_requests_mutex);
    pthread_mutex_destroy (&m_requests_mutex);

    // libevent cleanup
    event_base_free(m_base);
    m_base = NULL;
}

int CoreClient::createConnection(std::string hostname, int port) {
    struct bufferevent *bev = bufferevent_socket_new(m_base, -1, BEV_OPT_CLOSE_ON_FREE);
    if (bev == NULL) {
        return -1;
    }

    boost::shared_ptr<CxnContext> context(new CxnContext(bev, m_connCallback));
    context->client = this;
    context->base = m_base;
    context->hostname = hostname;
    context->port = port;

    pthread_mutex_lock(&m_contexts_mutex);
    m_contexts.push_back(context);
    pthread_mutex_unlock(&m_contexts_mutex);

    return event_base_once(m_base, -1, EV_TIMEOUT, authenticationRequestExtCallback, context.get(), NULL);
}

int CoreClient::invoke(Procedure &proc, voltdb_proc_callback callback, void *payload) {
    int32_t messageSize = proc.getSerializedSize();
    char *bbdata = new char[messageSize];
    ByteBuffer bb(bbdata, messageSize);
    proc.serializeTo(&bb, 0);
    PreparedInvocation invocation(bbdata, messageSize, callback, payload);

    pthread_mutex_lock(&m_requests_mutex);
    m_requests.push_back(invocation);
    pthread_mutex_unlock(&m_requests_mutex);

    return event_base_once(m_base, -1, EV_TIMEOUT, invocationRequestExtCallback, this, NULL);
}

int CoreClient::runOnce() {
    int result = 0;

    // use a try block to ensure
    try {
        result = event_base_loop(m_base,EVLOOP_NONBLOCK);
    }
    catch (voltdb::Exception &e) {
        result = -1;
    }
    if (result == -1) return -1;
    else return 0;
}

int CoreClient::run() {
    return runWithTimeout(1000LL * 60 * 60 * 24 * 365 * 3); // 3 YRs
}

/**
 * Callback for runWithTimeout to break out of the loop when the
 * timeout is over.
 */
static void timeout_callback(evutil_socket_t fd, short what, void *arg) {
    CoreClient *client = reinterpret_cast<CoreClient*>(arg);
    assert(client);
    client->timerFired();
}

void CoreClient::timerFired() {
    m_timerFired = true;
    event_base_loopbreak(m_base);
}

int CoreClient::runWithTimeout(int64_t ms) {
    // construct a timer event
    event *timer = event_new(m_base, -1, EV_TIMEOUT, timeout_callback, this);
    if (!timer) {
        return EVENT_LOOP_ERROR;
    }

    // set the timeout amount
    struct timeval t;
    t.tv_sec = ms / 1000;
    t.tv_usec = static_cast<int>((ms % 1000) * 1000);

    int result = 0;
    m_timerFired = false;

    // use a try block to ensure
    try {
        result = event_add(timer, &t);
        if (result != -1)
            result = event_base_dispatch(m_base);
    }
    catch (voltdb::Exception &e) {
        result = EVENT_LOOP_ERROR;
    }
    event_free(timer);

    // figure out why the loop exited
    if (result != EVENT_LOOP_ERROR) {
        if (m_timerFired) return TIMEOUT_ELAPSED;
        else return INTERRUPTED_OR_EARLY_EXIT;
    }

    return result;
}

void CoreClient::interrupt() {
    event_base_loopbreak(m_base);
}

void CoreClient::setData(void *data) {
    m_data = data;
}

void *CoreClient::getData() {
    return m_data;
}

void CoreClient::completeAuthenticationRequest(struct CxnContext *context) {

    assert(context->connected);

    bufferevent_setwatermark( context->bev, EV_READ, 4, HIGH_WATERMARK);
    bufferevent_setwatermark( context->bev, EV_WRITE, 8192, 262144);
    if (bufferevent_enable(context->bev, EV_READ)) {
        throw voltdb::LibEventException();
    }
    AuthenticationRequest authRequest( m_username, "database", m_passwordHash );
    ScopedByteBuffer bb(authRequest.getSerializedSize());
    authRequest.serializeTo(&bb);
    struct evbuffer *evbuf = bufferevent_get_output(context->bev);
    if (evbuffer_add( evbuf, bb.bytes(), static_cast<size_t>(bb.remaining()))) {
        throw voltdb::LibEventException();
    }
}

bool CoreClient::processAuthenticationResponse(struct CxnContext *context, AuthenticationResponse &response) {

    if (!m_instanceIdIsSet) {
        m_instanceIdIsSet = true;
        m_clusterStartTime = response.clusterStartTime();
        m_leaderAddress = response.leaderAddress();
    } else {
        if (m_clusterStartTime != response.clusterStartTime() ||
            m_leaderAddress != response.leaderAddress()) {
            return false;
        }
    }
    bufferevent_setwatermark( context->bev, EV_READ, 4, HIGH_WATERMARK);
    bufferevent_setcb(context->bev,
                      voltdb::regularReadCallback,
                      voltdb::regularWriteCallback,
                      voltdb::regularEventCallback,
                      context);
    return true;
}


void CoreClient::invocationRequestCallback() {
    size_t requestsPending = 0;
    PreparedInvocation invocation;

    while (true) {
        // get the invocation and the number queued
        pthread_mutex_lock(&m_requests_mutex);
        {
            requestsPending = m_requests.size();
            if (requestsPending != 0) {
                invocation = m_requests.front();
                m_requests.pop_front();
            }
        }
        pthread_mutex_unlock(&m_requests_mutex);

        // no work to be done; break out of the loop
        if (requestsPending == 0)
            return;

        CxnContext *context = getNextContext(invocation.callback, invocation.payload);
        // on failure, the callback should already have been called
        if (!context)
            return;

        // pick a new unique identifier and put it in the invocation
        int64_t client_token = m_nextRequestId++;
        ByteBuffer ibuf(invocation.data.get(), invocation.data_size);
        Procedure::updateClientData(&ibuf, client_token);

        // set up the callback
        CallbackPair callbackPair;
        callbackPair.callback = invocation.callback;
        callbackPair.payload = invocation.payload;
        context->callbacks[client_token] = callbackPair;
        context->outstanding++;

        struct evbuffer *evbuf = bufferevent_get_output(context->bev);
        m_outstandingRequests++;

        // try to write to the connection's buffer, call the callback on failure
        if (evbuffer_add(evbuf, invocation.data.get(), static_cast<size_t>(invocation.data_size))) {
            if (callbackPair.callback) {
                callbackPair.callback(context->client, InvocationResponse(), callbackPair.payload);
            }
            context->callbacks.erase(client_token);
            context->outstanding--;
            m_outstandingRequests--;
        }

        // if the buffer to go out gets too big
        if (evbuffer_get_length(evbuf) > 262144) {
            context->backpressure = true;

            connection_event event;
            event.hostname = context->hostname;
            event.port = context->port;
            event.type = BACKPRESSURE_ON;
            event.info = "Queued too much data for connection's TCP/IP buffer";
            context->connCallback(context->client, event);
        }

        // did all the work; break out of the loop without aquiring the lock again
        if (requestsPending == 1)
            return;
    }
}

CxnContext *CoreClient::getNextContext(voltdb_proc_callback callback, void *payload) {
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

    CxnContext *bestContext = NULL;
    int32_t maxOutstanding = -1;

    int backpressureConns = 0;
    int liveConns = 0;

    // find (if possible) the
    pthread_mutex_lock(&m_contexts_mutex);
    {
        std::vector<boost::shared_ptr<CxnContext> >::const_iterator iter;
        for (iter = m_contexts.begin(); iter != m_contexts.end(); iter++) {
            CxnContext *context = iter->get();
            if (context->authenticated == false) {
                continue;
            }
            ++liveConns;
            if (context->backpressure) {
                ++backpressureConns;
                continue;
            }
            if (context->outstanding > maxOutstanding) {
                maxOutstanding = context->outstanding;
                bestContext = context;
            }
        }
    }
    pthread_mutex_unlock(&m_contexts_mutex);

    // inform the user via callback there are no live connections
    if (liveConns == 0) {
        if (callback) {
            callback(this, InvocationResponse(), payload);
        }
        return NULL;
    }

    // inform the user via callback all connections are in backpressure mode
    if (!bestContext) {
        if (callback) {
            callback(this, InvocationResponse(), payload);
        }
        return NULL;
    }

    return bestContext;
}

void CoreClient::regularReadCallback(struct CxnContext *context) {
    struct evbuffer *evbuf = bufferevent_get_input(context->bev);
    int32_t remaining = static_cast<int32_t>(evbuffer_get_length(evbuf));

    if (context->lengthOrMessage && remaining < 4) {
        return;
    }

    while (true) {
        if (context->lengthOrMessage && remaining >= 4) {
            char lengthBytes[4];
            ByteBuffer lengthBuffer(lengthBytes, 4);
            evbuffer_remove( evbuf, lengthBytes, 4);
            context->nextLength = lengthBuffer.getInt32();
            context->lengthOrMessage = false;
            remaining -= 4;
        }
        else if (remaining >= context->nextLength) {
            boost::shared_array<char> messageBytes = boost::shared_array<char>(new char[context->nextLength]);
            context->lengthOrMessage = true;
            evbuffer_remove( evbuf, messageBytes.get(), static_cast<size_t>(context->nextLength));
            remaining -= context->nextLength;
            InvocationResponse response(messageBytes, context->nextLength);

            std::map<int64_t, CallbackPair>::iterator i = context->callbacks.find(response.clientData());
            CallbackPair callbackPair = i->second;
            if (callbackPair.callback) {
                callbackPair.callback(this, response, callbackPair.payload);
            }
            context->callbacks.erase(i);
            m_outstandingRequests--;
            context->outstanding--;

        } else {
            if (context->lengthOrMessage) {
                bufferevent_setwatermark( context->bev, EV_READ, 4, HIGH_WATERMARK);
            } else {
                bufferevent_setwatermark( context->bev, EV_READ, static_cast<size_t>(context->nextLength), HIGH_WATERMARK);
            }
            break;
        }
    }
}

void CoreClient::regularEventCallback(struct CxnContext *context, short events) {
    if (events & BEV_EVENT_CONNECTED) {
        assert(false);
    } else if (events & (BEV_EVENT_ERROR | BEV_EVENT_EOF)) {
        /*
         * First drain anything in the read buffer
         */
        regularReadCallback(context);

        // update status for sending
        context->authenticated = false;
        context->connected = false;

        // Iterate the list of callbacks for this connection and collect them
        std::map<int64_t, CallbackPair>::const_iterator iter;
        std::vector<CallbackPair> callbacks;
        for (iter = context->callbacks.begin(); iter != context->callbacks.end(); iter++) {
            callbacks.push_back(iter->second);
        }
        context->outstanding = 0;
        context->callbacks.clear();

        //setup the notification that a connection was lost
        connection_event event;
        event.type = CONNECTION_LOST;
        event.hostname = context->hostname;
        event.port = context->port;
        event.info = "Connection was lost.";

        // remove this context from the global set
        pthread_mutex_lock(&m_contexts_mutex);
        {
            for (std::vector<boost::shared_ptr<CxnContext> >::iterator i = m_contexts.begin(); i != m_contexts.end(); i++) {
                if (i->get() == context) {
                    m_contexts.erase(i);
                    break;
                }
            }
        }
        pthread_mutex_unlock(&m_contexts_mutex);

        // notify the client the connection is dropped
        m_connCallback(this, event);

        // call callbacks for outstanding txns
        std::vector<CallbackPair>::const_iterator callbackIter;
        for (callbackIter = callbacks.begin(); callbackIter != callbacks.end(); callbackIter++) {
            --m_outstandingRequests;
            callbackIter->callback(this, InvocationResponse(), callbackIter->payload);
        }
    }
}

void CoreClient::regularWriteCallback(struct CxnContext *context) {
    if (context->backpressure) {
        context->backpressure = false;

        connection_event event;
        event.type = CONNECTION_LOST;
        event.hostname = context->hostname;
        event.port = context->port;
        event.info = "Connection was lost.";
        m_connCallback(this, event);
    }
}

}
