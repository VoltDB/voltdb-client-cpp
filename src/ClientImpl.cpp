/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
#include "ClientImpl.h"
#include <cassert>
#include "AuthenticationResponse.hpp"
#include "AuthenticationRequest.hpp"
#include <event2/buffer.h>
#include <event2/thread.h>
#include <event2/event.h>
#include "sha1.h"
#include "sha256.h"
#include <boost/foreach.hpp>
#include <sstream>

#define HIGH_WATERMARK 1024 * 1024 * 55
#define RECONNECT_INTERVAL 10

namespace voltdb {

#ifdef DEBUG
static bool voltdb_clientimpl_debug_init_libevent = false;
#endif

int64_t get_sec_time() {
    struct timeval tp;
    int res = gettimeofday(&tp, NULL);
    assert(res == 0);
    return tp.tv_sec;
}

class PendingConnection {
public:
    PendingConnection(const std::string& hostname,const unsigned short port, const bool keepConnecting, struct event_base *base, ClientImpl* ci)
        :  m_hostname(hostname), m_port(port), m_keepConnecting(keepConnecting), m_base(base), m_authenticationResponseLength(-1),
           m_status(true), m_loginExchangeCompleted(false), m_startPending(-1), m_ci(ci) {
    }

    void initiateAuthentication(struct bufferevent *bev) {
       m_ci->initiateAuthentication(this, bev);
    }

    void finalizeAuthentication(struct bufferevent *bev) {
        m_ci->finalizeAuthentication(this, bev);
    }

    ~PendingConnection() {}

    /*
     * Host and port of pending connection
     * */
    const std::string m_hostname;
    const unsigned short m_port;
    const bool m_keepConnecting;

    /*
     *Event and event base associated with connection
     */
    struct event_base *m_base;
    int32_t m_authenticationResponseLength;
    AuthenticationResponse m_response;
    bool m_status;
    bool m_loginExchangeCompleted;
    int64_t m_startPending;
    ClientImpl* m_ci;
};

typedef boost::shared_ptr<PendingConnection> PendingConnectionSPtr;

class CxnContext {
/*
 * Data associated with a specific connection
 */
public:
    CxnContext(const std::string& name, unsigned short port) : m_name(name), m_port(port), m_nextLength(4), m_lengthOrMessage(true) {

    }
    const std::string m_name;
    const unsigned short m_port;
    int32_t m_nextLength;
    bool m_lengthOrMessage;
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
static void authenticationReadCallback(struct bufferevent *bev, void *ctx) {
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
            bufferevent_setwatermark( bev, EV_READ, static_cast<size_t>(messageLength), HIGH_WATERMARK);
            return;
        }
    }

    ScopedByteBuffer buffer(pc->m_authenticationResponseLength);
    int read = evbuffer_remove(evbuf, buffer.bytes(), static_cast<size_t>(pc->m_authenticationResponseLength));
    assert(read == pc->m_authenticationResponseLength);
    AuthenticationResponse r = AuthenticationResponse(buffer);
    if (!r.success()) {
        pc->m_authenticationResponseLength =-1;
        return;
    }
    pc->m_response = r;
    pc->m_loginExchangeCompleted = true;

    bufferevent_setwatermark( bev, EV_READ, 4, HIGH_WATERMARK);
    pc->finalizeAuthentication(bev);

    if (pc->m_startPending < 0) {
        //connection is pending from regular createConeection API
        event_base_loopexit(pc->m_base, NULL);
    } else {
        pc->m_startPending = get_sec_time();
    }
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
    PendingConnection *pc = reinterpret_cast<PendingConnection*>(ctx);
    if (events & BEV_EVENT_CONNECTED) {
        pc->initiateAuthentication(bev);
    } else if (events & (BEV_EVENT_ERROR | BEV_EVENT_EOF)) {
        pc->m_status = false;
        //pc->m_loginExchangeCompleted = true;
        if (bev)
            bufferevent_free(bev);
    }

    if (pc->m_startPending < 0) {
        //connection is pending from regular createConeection API
        event_base_loopexit(pc->m_base, NULL);
    } else {
        pc->m_startPending = get_sec_time();
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
static void regularReadCallback(struct bufferevent *bev, void *ctx) {
    ClientImpl *impl = reinterpret_cast<ClientImpl*>(ctx);
    impl->regularReadCallback(bev);
}

void wakeupPipeCallback(evutil_socket_t fd, short what, void *ctx) {
    ClientImpl *impl = reinterpret_cast<ClientImpl*>(ctx);
    char buf[64];
    read(fd, buf, sizeof buf);
    impl->eventBaseLoopBreak();
}

/*
 * Only has to handle the case where there is an error or EOF
 */
static void regularEventCallback(struct bufferevent *bev, short events, void *ctx) {
    ClientImpl *impl = reinterpret_cast<ClientImpl*>(ctx);
    impl->regularEventCallback(bev, events);
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
    ClientImpl *impl = reinterpret_cast<ClientImpl*>(ctx);
    impl->regularWriteCallback(bev);
}

ClientImpl::~ClientImpl() {
    for (std::vector<struct bufferevent *>::iterator i = m_bevs.begin(); i != m_bevs.end(); ++i) {
        bufferevent_free(*i);
    }
    m_bevs.clear();
    m_contexts.clear();
    m_callbacks.clear();
    if (m_passwordHash != NULL) free(m_passwordHash);
    event_base_free(m_base);
    if (m_wakeupPipe[1] != -1) {
       ::close(m_wakeupPipe[0]);
       ::close(m_wakeupPipe[1]);
    }
}

// Initialization for the library that only gets called once
pthread_once_t once_initLibevent = PTHREAD_ONCE_INIT;
void initLibevent() {
    // try to run threadsafe libevent
    if (evthread_use_pthreads()) {
        throw voltdb::LibEventException();
    }
}

const int64_t ClientImpl::VOLT_NOTIFICATION_MAGIC_NUMBER(9223372036854775806);

ClientImpl::ClientImpl(ClientConfig config) throw(voltdb::Exception, voltdb::LibEventException) :
        m_nextRequestId(INT64_MIN), m_nextConnectionIndex(0), m_listener(config.m_listener),
        m_invocationBlockedOnBackpressure(false), m_loopBreakRequested(false), m_isDraining(false),
        m_instanceIdIsSet(false), m_outstandingRequests(0), m_username(config.m_username),
        m_maxOutstandingRequests(config.m_maxOutstandingRequests), m_ignoreBackpressure(false),
        m_useClientAffinity(false),m_updateHashinator(false), m_pendingConnectionSize(0) ,
        m_pLogger(0)
{

    pthread_once(&once_initLibevent, initLibevent);
#ifdef DEBUG
    if (!voltdb_clientimpl_debug_init_libevent) {
        event_enable_debug_mode();
        voltdb_clientimpl_debug_init_libevent = true;
    }
#endif
    struct event_config *cfg = event_config_new();
    event_config_set_flag(cfg, EVENT_BASE_FLAG_NO_CACHE_TIME);//, EVENT_BASE_FLAG_NOLOCK);
    m_base = event_base_new_with_config(cfg);
    assert(m_base);
    if (!m_base) {
        throw voltdb::LibEventException();
    }
    m_enableAbandon = config.m_enableAbandon;
    m_hashScheme = config.m_hashScheme;
    if (m_hashScheme == HASH_SHA1) {
        SHA1_CTX context;
        SHA1_Init(&context);
        SHA1_Update( &context, reinterpret_cast<const unsigned char*>(config.m_password.data()), config.m_password.size());
        m_passwordHash = (unsigned char *)malloc(20*sizeof(char));
        SHA1_Final ( &context, m_passwordHash);
    } else if (config.m_hashScheme == HASH_SHA256) {
        m_passwordHash = (unsigned char *)malloc(32*sizeof(char));
        computeSHA256(config.m_password.c_str(), config.m_password.size(), m_passwordHash);
    } else {
        throw voltdb::LibEventException();
    }

}

class FreeBEVOnFailure {
public:
    FreeBEVOnFailure(struct bufferevent *bev) : m_bev(bev) {

    }
    ~FreeBEVOnFailure() {
        if (m_bev) {
            bufferevent_free(m_bev);
        }
    }
    void success() {
        m_bev = NULL;
    }
private:
    struct bufferevent *m_bev;
};

void ClientImpl::initiateConnection(boost::shared_ptr<PendingConnection> &pc) throw (voltdb::ConnectException, voltdb::LibEventException){
    std::stringstream ss;
    ss << "ClientImpl::initiateConnection to " << pc->m_hostname << ":" << pc->m_port;
    logMessage(ClientLogger::INFO, ss.str());
    struct bufferevent *bev = bufferevent_socket_new(m_base, -1, BEV_OPT_CLOSE_ON_FREE | BEV_OPT_THREADSAFE);
    if (bev == NULL) {
        if (pc->m_keepConnecting) {
            createPendingConnection(pc->m_hostname, pc->m_port);
        } else {
            throw ConnectException();
        }
    }
    FreeBEVOnFailure protector(bev);
    bufferevent_setcb(bev, authenticationReadCallback, NULL, authenticationEventCallback, pc.get());

    if (bufferevent_socket_connect_hostname(bev, NULL, AF_INET, pc->m_hostname.c_str(), pc->m_port) != 0) {
        if (pc->m_keepConnecting) {
            createPendingConnection(pc->m_hostname, pc->m_port);
        } else {
            ss.str("");
            ss << "!!!! ClientImpl::initiateConnection to " << pc->m_hostname << ":" << pc->m_port << " failed";
            logMessage(ClientLogger::ERROR, ss.str());

            throw voltdb::LibEventException();
        }
    }
    protector.success();
}

void ClientImpl::close() {
    //drain before we close;
    drain();
    if (m_wakeupPipe[1] != -1) {
       ::close(m_wakeupPipe[0]);
       ::close(m_wakeupPipe[1]);
    }
    if (m_bevs.empty()) return;
    for (std::vector<struct bufferevent *>::iterator i = m_bevs.begin(); i != m_bevs.end(); ++i) {
        int fd = bufferevent_getfd(*i);
        evutil_closesocket(fd);
        bufferevent_free(*i);
    }
    m_bevs.clear();
}

void ClientImpl::initiateAuthentication(PendingConnection* pc, struct bufferevent *bev)throw (voltdb::LibEventException) {

    logMessage(ClientLogger::DEBUG, "ClientImpl::initiateAuthentication");

    FreeBEVOnFailure protector(bev);
    bufferevent_setwatermark( bev, EV_READ, 4, HIGH_WATERMARK);
    bufferevent_setwatermark( bev, EV_WRITE, 8192, 262144);

    if (bufferevent_enable(bev, EV_READ)) {
        throw voltdb::LibEventException();
    }
    AuthenticationRequest authRequest( m_username, "database", m_passwordHash, m_hashScheme );
    int size = authRequest.getSerializedSize();
    ScopedByteBuffer bb(authRequest.getSerializedSize());
    authRequest.serializeTo(&bb);

    struct evbuffer *evbuf = bufferevent_get_output(bev);
    if (evbuffer_add( evbuf, bb.bytes(), static_cast<size_t>(bb.remaining()))) {
        throw voltdb::LibEventException();
    }
    protector.success();
        }

void ClientImpl::finalizeAuthentication(PendingConnection* pc, struct bufferevent *bev) throw (voltdb::Exception, voltdb::ConnectException){

    logMessage(ClientLogger::DEBUG, "ClientImpl::finalizeAuthentication");

    FreeBEVOnFailure protector(bev);
    if (pc->m_loginExchangeCompleted) {

        logMessage(ClientLogger::DEBUG, "ClientImpl::finalizeAuthentication OK");

        if (!m_instanceIdIsSet) {
            m_instanceIdIsSet = true;
            m_clusterStartTime = pc->m_response.clusterStartTime();
            m_leaderAddress = pc->m_response.leaderAddress();
        } else {
            if (m_clusterStartTime != pc->m_response.clusterStartTime() ||
                m_leaderAddress != pc->m_response.leaderAddress()) {
                throw ClusterInstanceMismatchException();
            }
        }
        //save event for host id
        m_hostIdToEvent[pc->m_response.hostId()] = bev;
        bufferevent_setwatermark( bev, EV_READ, 4, HIGH_WATERMARK);
        m_bevs.push_back(bev);

        m_contexts[bev] =
               boost::shared_ptr<CxnContext>(
                   new CxnContext(pc->m_hostname, pc->m_port));
        boost::shared_ptr<CallbackMap> callbackMap(new CallbackMap());
        m_callbacks[bev] = callbackMap;

        bufferevent_setcb(
               bev,
               voltdb::regularReadCallback,
               voltdb::regularWriteCallback,
               voltdb::regularEventCallback, this);

        {
            boost::mutex::scoped_lock lock(m_pendingConnectionLock);
            for (std::list<PendingConnectionSPtr>::iterator i = m_pendingConnectionList.begin(); i != m_pendingConnectionList.end(); ++i) {
                if (i->get() == pc) {
                    m_pendingConnectionList.erase(i);
                    m_pendingConnectionSize.store(m_pendingConnectionList.size(), boost::memory_order_release);
                    break;
                }
            }
        }

        //update topology info and procedures info
        if (m_useClientAffinity) {
            updateHashinator();
            subscribeToTopologyNotifications();
        }

    	std::stringstream ss;
    	ss << "connectionActive " << m_contexts[bev]->m_name << ":" << m_contexts[bev]->m_port ;
    	logMessage(ClientLogger::INFO, ss.str());

        //Notify client that a connection was active
        if (m_listener.get() != NULL) {
            try {

                 m_listener->connectionActive( m_contexts[bev]->m_name, m_bevs.size() );
            } catch (const std::exception& e) {
                std::cerr << "Status listener threw exception on connection active: " << e.what() << std::endl;
            }
        }

    }
    else {


        logMessage(ClientLogger::DEBUG, "ClientImpl::finalizeAuthentication Fail");

    	std::stringstream ss;
    	ss << "connection failed " << " " << pc->m_hostname << ":" << pc->m_port;
    	logMessage(ClientLogger::ERROR, ss.str());

        throw ConnectException();
    }
            protector.success();
}

void ClientImpl::createConnection(const std::string& hostname, const unsigned short port, const bool keepConnecting) throw (voltdb::Exception, voltdb::ConnectException, voltdb::LibEventException) {

    std::stringstream ss;
    ss << "ClientImpl::createConnection" << " hostname:" << hostname << " port:" << port;
    logMessage(ClientLogger::INFO, ss.str());

    if (0 == pipe(m_wakeupPipe)) {
        struct event *ev = event_new(m_base, m_wakeupPipe[0], EV_READ|EV_PERSIST, wakeupPipeCallback, this);
        event_add(ev, NULL);
    } else {
        m_wakeupPipe[1] = -1;
    }
    PendingConnectionSPtr pc(new PendingConnection(hostname, port, keepConnecting, m_base, this));
    initiateConnection(pc);

    if (event_base_dispatch(m_base) == -1) {
        throw voltdb::LibEventException();
    }

    if (pc->m_status) {
        if (event_base_dispatch(m_base) == -1) {
            throw voltdb::LibEventException();
        }

        if (pc->m_loginExchangeCompleted) {
            return;
        }
    }
    if (keepConnecting) {
        createPendingConnection(hostname, port);
    } else {
        throw ConnectException();
    }
}

static void reconnectCallback(evutil_socket_t fd, short events, void *clientData) {
    ClientImpl *self = reinterpret_cast<ClientImpl*>(clientData);
    self->reconnectEventCallback();
}

void ClientImpl::reconnectEventCallback() {
    if (m_pendingConnectionSize.load(boost::memory_order_consume) <= 0)  return;

    boost::mutex::scoped_lock lock(m_pendingConnectionLock);
    const int64_t now = get_sec_time();
    BOOST_FOREACH( PendingConnectionSPtr& pc, m_pendingConnectionList ) {
        if ((now - pc->m_startPending) > RECONNECT_INTERVAL){
            //pc->m_startPending = now;
            initiateConnection(pc);
        }
    }

    struct timeval tv;
    tv.tv_sec = RECONNECT_INTERVAL;
    tv.tv_usec = 0;

    event_base_once(m_base, -1, EV_TIMEOUT, reconnectCallback, this, &tv);
}

void ClientImpl::createPendingConnection(const std::string &hostname, const unsigned short port, int64_t time) {

    logMessage(ClientLogger::DEBUG, "ClientImpl::createPendingConnection");

    PendingConnectionSPtr pc(new PendingConnection(hostname, port, false, m_base, this));
    pc->m_startPending = time;
    {
        boost::mutex::scoped_lock lock(m_pendingConnectionLock);
        m_pendingConnectionList.push_back(pc);
        m_pendingConnectionSize.store(m_pendingConnectionList.size(), boost::memory_order_release);
    }

    struct timeval tv;
    tv.tv_sec = (time > 0)? RECONNECT_INTERVAL: 0;
    tv.tv_usec = 0;

    event_base_once(m_base, -1, EV_TIMEOUT, reconnectCallback, this, &tv);
}


/*
 * A synchronous callback returns the invocation response to the provided address
 * and requests the event loop break
 */
class SyncCallback : public ProcedureCallback {
public:
    SyncCallback(InvocationResponse *responseOut) : m_responseOut(responseOut) {
    }

    bool callback(InvocationResponse response) throw (voltdb::Exception) {
        (*m_responseOut) = response;
        return true;
    }

    void abandon(AbandonReason reason) {}

private:
    InvocationResponse *m_responseOut;
};

InvocationResponse ClientImpl::invoke(Procedure &proc) throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::UninitializedParamsException, voltdb::LibEventException) {
    if (m_bevs.empty()) {
        throw voltdb::NoConnectionsException();
    }
    int32_t messageSize = proc.getSerializedSize();
    ScopedByteBuffer sbb(messageSize);
    int64_t clientData = m_nextRequestId++;
    proc.serializeTo(&sbb, clientData);
    struct bufferevent *bev = m_bevs[m_nextConnectionIndex++ % m_bevs.size()];
    InvocationResponse response;
    boost::shared_ptr<ProcedureCallback> callback(new SyncCallback(&response));
    struct evbuffer *evbuf = bufferevent_get_output(bev);
    if (evbuffer_add(evbuf, sbb.bytes(), static_cast<size_t>(sbb.remaining()))) {
        throw voltdb::LibEventException();
    }
    m_outstandingRequests++;
    (*m_callbacks[bev])[clientData] = callback;
    if (event_base_dispatch(m_base) == -1) {
        throw voltdb::LibEventException();
    }
    m_loopBreakRequested = false;
    return response;
}

class DummyCallback : public ProcedureCallback {
public:
    ProcedureCallback *m_callback;
    DummyCallback(ProcedureCallback *callback) : m_callback(callback) {}
    bool callback(InvocationResponse response) throw (voltdb::Exception) {
        return m_callback->callback(response);
    }

    void abandon(AbandonReason reason) {}

};

void ClientImpl::invoke(Procedure &proc, ProcedureCallback *callback) throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::UninitializedParamsException, voltdb::LibEventException, voltdb::ElasticModeMismatchException) {
    boost::shared_ptr<ProcedureCallback> wrapper(new DummyCallback(callback));
    invoke(proc, wrapper);
}

struct bufferevent *ClientImpl::routeProcedure(Procedure &proc, ScopedByteBuffer &sbb){
    ProcedureInfo *procInfo = m_distributer.getProcedure(proc.getName());

    //route transaction to correct event if procedure is found, transaction is single partitioned
    int hostId = -1;
    if (procInfo && !procInfo->m_multiPart){
        const int hashedPartition = m_distributer.getHashedPartitionForParameter(sbb, procInfo->m_partitionParameter);
        if (hashedPartition >= 0) {
            hostId = m_distributer.getHostIdByPartitionId(hashedPartition);
        }
    }
    else
    {
        //use MIP partition instead
        hostId = m_distributer.getHostIdByPartitionId(Distributer::MP_INIT_PID);
    }
    if (hostId >= 0) {
        struct bufferevent *bev = m_hostIdToEvent[hostId];
        return bev;
    }
    return NULL;
}


void ClientImpl::invoke(Procedure &proc, boost::shared_ptr<ProcedureCallback> callback) throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::UninitializedParamsException, voltdb::LibEventException, voltdb::ElasticModeMismatchException) {
    if (callback.get() == NULL) {
        throw voltdb::NullPointerException();
    }
    if (m_bevs.empty()) {
        throw voltdb::NoConnectionsException();
    }

    if (m_outstandingRequests >= m_maxOutstandingRequests) {
    	if (m_listener.get() != NULL) {
            try {
                m_listener->backpressure(true);
            } catch (const std::exception& e) {
                std::cerr << "Exception thrown on invocation of backpressure callback: " << e.what() << std::endl;
            }
        }
    	// We are overloaded, we need to reject traffic and notify the caller
        if (m_enableAbandon) {
            callback->abandon(ProcedureCallback::TOO_BUSY);
            return;
        }
    }

    //do not call the procedures if hashinator is in the LEGACY mode
    if (!m_distributer.isUpdating() && !m_distributer.isElastic()) {
        //todo: need to remove the connection
        throw voltdb::ElasticModeMismatchException();
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
        if (m_ignoreBackpressure) {
            bev = m_bevs[++m_nextConnectionIndex % m_bevs.size()];
            break;
        }

        //Assume backpressure if the number of outstanding requests is too large, i.e. leave bev == NULL
        if (m_outstandingRequests <= m_maxOutstandingRequests) {
            for (size_t ii = 0; ii < m_bevs.size(); ii++) {
                bev = m_bevs[++m_nextConnectionIndex % m_bevs.size()];
        	if (m_backpressuredBevs.find(bev) != m_backpressuredBevs.end()) {
        	    bev = NULL;
        	} else {
        	    break;
        	}
            }
	}

    	if (bev) {
    	    break;
    	} else {
    	    bool callEventLoop = true;
    	    if (m_listener.get() != NULL) {
    	        try {
    	            m_ignoreBackpressure = true;
    	            callEventLoop = !m_listener->backpressure(true);
    	            m_ignoreBackpressure = false;
                } catch (const std::exception& e) {
    	            std::cerr << "Exception thrown on invocation of backpressure callback: " << e.what() << std::endl;
    	        }
    	    }
            if (callEventLoop) {
    	        m_invocationBlockedOnBackpressure = true;
    	        if (event_base_dispatch(m_base) == -1) {
    	            throw voltdb::LibEventException();
    	        }
    	        if (m_loopBreakRequested) {
    	            m_loopBreakRequested = false;
    	            m_invocationBlockedOnBackpressure = false;
    	            bev = m_bevs[++m_nextConnectionIndex % m_bevs.size()];
    	        }
    	    } else {
    	        bev = m_bevs[++m_nextConnectionIndex % m_bevs.size()];
    	        break;
            }
        }
    }

    //route transaction to correct event if client affinity is enabled and hashinator updating is not in progress
    //elastic scalability is disabled
    if (m_useClientAffinity && !m_distributer.isUpdating()) {
        struct bufferevent *routed_bev = routeProcedure(proc, sbb);
        // Check if the routed_bev is valid and has not been removed due to lost connection
        if ((routed_bev) && (m_callbacks.find(routed_bev) != m_callbacks.end()))
            bev = routed_bev;
    }

    struct evbuffer *evbuf = bufferevent_get_output(bev);
    if (evbuffer_add(evbuf, sbb.bytes(), static_cast<size_t>(sbb.remaining()))) {
        throw voltdb::LibEventException();
    }
    m_outstandingRequests++;
    (*m_callbacks[bev])[clientData] = callback;

    if (evbuffer_get_length(evbuf) >  262144) {
        m_backpressuredBevs.insert(bev);
    }

    return;
}

void ClientImpl::runOnce() throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::LibEventException) {

    logMessage(ClientLogger::DEBUG, "ClientImpl::runOnce");

    if (m_bevs.empty() && m_pendingConnectionSize.load(boost::memory_order_consume) <= 0) {
        throw voltdb::NoConnectionsException();
    }

    if (event_base_loop(m_base, EVLOOP_NONBLOCK) == -1) {
        throw voltdb::LibEventException();
    }
    m_loopBreakRequested = false;
}

void ClientImpl::run() throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::LibEventException) {

    logMessage(ClientLogger::DEBUG, "ClientImpl::run");

    if (m_bevs.empty() && m_pendingConnectionSize.load(boost::memory_order_consume) <= 0) {
        throw voltdb::NoConnectionsException();
    }
    if (event_base_dispatch(m_base) == -1) {
        throw voltdb::LibEventException();
    }

    m_loopBreakRequested = false;
}

void ClientImpl::regularReadCallback(struct bufferevent *bev) {
    struct evbuffer *evbuf = bufferevent_get_input(bev);
    boost::shared_ptr<CxnContext> context = m_contexts[bev];
    int32_t remaining = static_cast<int32_t>(evbuffer_get_length(evbuf));
    bool breakEventLoop = false;

    if (context->m_lengthOrMessage && remaining < 4) {
        return;
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
            InvocationResponse response(messageBytes, context->m_nextLength);
            if (response.clientData() == VOLT_NOTIFICATION_MAGIC_NUMBER) {
                if (!response.failure()){
                    m_distributer.handleTopologyNotification(response.results());
                }
            } else {
                boost::shared_ptr<CallbackMap> callbackMap = m_callbacks[bev];
                CallbackMap::iterator i = callbackMap->find(response.clientData());
                if(i != callbackMap->end()){
                    try {
                        m_ignoreBackpressure = true;
                        breakEventLoop |= i->second->callback(response);
                        m_ignoreBackpressure = false;
                    } catch (std::exception &e) {
                        if (m_listener.get() != NULL) {
                            try {
                                m_ignoreBackpressure = true;
                                breakEventLoop |= m_listener->uncaughtException( e, i->second, response);
                                m_ignoreBackpressure = false;
                            } catch (const std::exception& e) {
                                std::cerr << "Uncaught exception handler threw exception: " << e.what() << std::endl;
                            }
                        }
                    }
                    callbackMap->erase(i);
                    m_outstandingRequests--;
                }
            }

            //If the client is draining and it just drained the last request, break the loop
            if (m_isDraining && m_outstandingRequests == 0) {
                breakEventLoop = true;
            } else if (m_loopBreakRequested && (m_outstandingRequests <= m_maxOutstandingRequests)) {
                // ignore break requested until we have too many outstanding requests
                breakEventLoop = true;
            }
        } else {
            if (context->m_lengthOrMessage) {
                bufferevent_setwatermark( bev, EV_READ, 4, HIGH_WATERMARK);
            } else {
                bufferevent_setwatermark( bev, EV_READ, static_cast<size_t>(context->m_nextLength), HIGH_WATERMARK);
            }
            breakEventLoop |= (m_loopBreakRequested && (m_outstandingRequests <= m_maxOutstandingRequests));
            break;
        }
    }

    if (breakEventLoop) {
        event_base_loopbreak( m_base );
    }
}
void ClientImpl::regularEventCallback(struct bufferevent *bev, short events) {
    if (events & BEV_EVENT_CONNECTED) {
        assert(false);
    } else if (events & (BEV_EVENT_ERROR | BEV_EVENT_EOF)) {
        /*
         * First drain anything in the read buffer
         */
        regularReadCallback(bev);

        bool breakEventLoop = false;

    	std::stringstream ss;
    	const char* s_error = events & BEV_EVENT_ERROR ? "BEV_EVENT_ERROR" : "BEV_EVENT_EOF";
    	ss << "connectionLost: " << s_error;
    	logMessage(ClientLogger::ERROR, ss.str());

        //Notify client that a connection was lost
        if (m_listener.get() != NULL) {
            try {
                m_ignoreBackpressure = true;
                breakEventLoop |= m_listener->connectionLost( m_contexts[bev]->m_name, m_bevs.size() - 1);
                m_ignoreBackpressure = false;
            } catch (const std::exception& e) {
                std::cerr << "Status listener threw exception on connection lost: " << e.what() << std::endl;
            }
        }
        /*
         * Iterate the list of callbacks for this connection and invoke them
         * with the appropriate error response
         */
        BEVToCallbackMap::iterator callbackMapIter = m_callbacks.find(bev);
        boost::shared_ptr<CallbackMap> callbackMap = callbackMapIter->second;
        for (CallbackMap::iterator i =  callbackMap->begin();
                i != callbackMap->end(); ++i) {
            try {
                breakEventLoop |=
                        i->second->callback(InvocationResponse());
            } catch (std::exception &e) {
                if (m_listener.get() != NULL) {
                    breakEventLoop |= m_listener->uncaughtException( e, i->second, InvocationResponse());
                }
            }
            m_outstandingRequests--;
        }

        if (m_isDraining && m_outstandingRequests == 0) {
            breakEventLoop = true;
        }

        //Remove the callback map from the callbacks map
        m_callbacks.erase(callbackMapIter);

        //remove the entry for the backpressured connection set
        m_backpressuredBevs.erase(bev);
        createPendingConnection(m_contexts[bev]->m_name, m_contexts[bev]->m_port, get_sec_time());

        //Remove the connection context
        m_contexts.erase(bev);

        for (std::vector<struct bufferevent *>::iterator i = m_bevs.begin(); i != m_bevs.end(); ++i) {
            if (*i == bev) {
                m_bevs.erase(i);
                break;
            }
        }

        bufferevent_free(bev);
        //Reset cluster Id as no more connections left
        if (m_bevs.empty())
            m_instanceIdIsSet = false;

        if (breakEventLoop || m_bevs.size() == 0) {
            event_base_loopbreak( m_base );
        }

        //update topology info and procedures info
        if (m_useClientAffinity && m_bevs.size() > 0) {
            updateHashinator();
        }
    }
}

void ClientImpl::regularWriteCallback(struct bufferevent *bev) {
    std::set<struct bufferevent*>::iterator iter =
            m_backpressuredBevs.find(bev);
    if (iter != m_backpressuredBevs.end()) {
        m_backpressuredBevs.erase(iter);
    }
    if (m_invocationBlockedOnBackpressure) {
        m_invocationBlockedOnBackpressure = false;
        event_base_loopbreak(m_base);
    }
}

static void interrupt_callback(evutil_socket_t fd, short events, void *clientData) {
    ClientImpl *self = reinterpret_cast<ClientImpl*>(clientData);
    self->eventBaseLoopBreak();
}

void ClientImpl::eventBaseLoopBreak() {
    event_base_loopbreak(m_base);
}

void ClientImpl::interrupt() {
    event_base_once(m_base, -1, EV_TIMEOUT, interrupt_callback, this, NULL);
}

/*
 * Enter the event loop and process pending events until all responses have been received and then return.
 * @throws NoConnectionsException No connections to the database so there is no work to be done
 * @throws LibEventException An unknown error occured in libevent
 */
bool ClientImpl::drain() throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::LibEventException) {
    if (m_outstandingRequests > 0) {
        m_isDraining = true;
        run();
    }

    return m_outstandingRequests == 0;
}


/*
 *Callback for async topology update for transaction routing algorithm
 */
class TopoUpdateCallback : public voltdb::ProcedureCallback
{
public:
    TopoUpdateCallback(Distributer *dist):m_dist(dist){}
    bool callback(InvocationResponse response) throw (voltdb::Exception)
    {
        if (response.failure()){
            //TODO:log
            return false;
        }
        m_dist->updateAffinityTopology(response.results());
        return true;
    }
 private:
    Distributer *m_dist;
};

class SubscribeCallback : public voltdb::ProcedureCallback
{
public:
    SubscribeCallback(){}
    bool callback(InvocationResponse response) throw (voltdb::Exception)
    {
        if (response.failure()){
            //TODO:log
            std::cout << "subscribeToTopoNotifications FAILED" << std::endl;
            return false;
        }
        return true;
    }
};

/*
 * Callback for ("@SystemCatalog", "PROCEDURES")
 */
class ProcUpdateCallback : public voltdb::ProcedureCallback
{
public:
    ProcUpdateCallback(Distributer *dist):m_dist(dist){}
    bool callback(InvocationResponse response) throw (voltdb::Exception)
    {
        if (response.failure()){
            //TODO:log
            return false;
        }
        m_dist->updateProcedurePartitioning(response.results());
        return true;
    }

 private:
    Distributer *m_dist;
};



void ClientImpl::updateHashinator(){
    m_distributer.startUpdate();
    std::vector<voltdb::Parameter> parameterTypes(1);
    parameterTypes[0] = voltdb::Parameter(voltdb::WIRE_TYPE_STRING);
    voltdb::Procedure systemCatalogProc("@SystemCatalog", parameterTypes);
    voltdb::ParameterSet* params = systemCatalogProc.params();
    params->addString("PROCEDURES");

    boost::shared_ptr<ProcUpdateCallback> procCallback(new ProcUpdateCallback(&m_distributer));
    invoke(systemCatalogProc, procCallback);

    parameterTypes.resize(2);
    parameterTypes[0] = voltdb::Parameter(voltdb::WIRE_TYPE_STRING);
    parameterTypes[1] = voltdb::Parameter(voltdb::WIRE_TYPE_INTEGER);

    voltdb::Procedure statisticsProc("@Statistics", parameterTypes);
    params = statisticsProc.params();
    params->addString("TOPO").addInt32(0);

    boost::shared_ptr<TopoUpdateCallback> topoCallback(new TopoUpdateCallback(&m_distributer));

    invoke(statisticsProc, topoCallback);
}

void ClientImpl::subscribeToTopologyNotifications(){
    std::vector<voltdb::Parameter> parameterTypes(1);
    parameterTypes[0] = voltdb::Parameter(voltdb::WIRE_TYPE_STRING);
    //parameterTypes[1] = voltdb::Parameter(voltdb::WIRE_TYPE_INTEGER);
    voltdb::Procedure statisticsProc("@Subscribe", parameterTypes);
    voltdb::ParameterSet* params = statisticsProc.params();
    params->addString("TOPOLOGY");

    boost::shared_ptr<SubscribeCallback> topoCallback(new SubscribeCallback());

    invoke(statisticsProc, topoCallback);
}

void ClientImpl::setClientAffinity(bool enable){
    if(enable && !m_useClientAffinity && !m_bevs.empty()) {
        updateHashinator();
        subscribeToTopologyNotifications();
    }
    m_useClientAffinity = enable;
}

void ClientImpl::wakeup() {
   if (m_wakeupPipe[1] != -1) {
        static unsigned char c = 'w';
        boost::mutex::scoped_lock lock(m_wakeupPipeLock, boost::try_to_lock);
        if (lock) write(m_wakeupPipe[1], &c, 1);
    } else {
      event_base_loopbreak(m_base);
    }
}

void ClientImpl::logMessage(ClientLogger::CLIENT_LOG_LEVEL severity, const std::string& msg){
    if( m_pLogger ){
        m_pLogger->log(severity, msg);
    }
}

}
