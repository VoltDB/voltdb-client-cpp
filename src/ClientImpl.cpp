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
#include "ClientImpl.h"
#include <cassert>
#include "AuthenticationResponse.hpp"
#include "AuthenticationRequest.hpp"
#include <event2/buffer.h>
#include <event2/thread.h>
#include <event2/event.h>
#include <boost/foreach.hpp>
#include <sstream>
#include <openssl/err.h>

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
    PendingConnection(const std::string& hostname, const unsigned short port, const bool keepConnecting,
                      struct event_base *base, ClientImpl* ci) : m_hostname(hostname),
                                                                 m_port(port),
                                                                 m_keepConnecting(keepConnecting),
                                                                 m_base(base),
                                                                 m_bufferEvent(NULL),
                                                                 m_authenticationResponseLength(-1),
                                                                 m_status(true),
                                                                 m_loginExchangeCompleted(false),
                                                                 m_startPending(-1),
                                                                 m_clientImpl(ci) {
    }

    void initiateAuthentication(struct bufferevent *bev) {
       m_clientImpl->initiateAuthentication(bev, m_hostname, m_port);
    }

    void finalizeAuthentication() {
        return m_clientImpl->finalizeAuthentication(this);
    }

    void cleanupBev() {
        if (m_bufferEvent) {
            bufferevent_free(m_bufferEvent);
            m_bufferEvent = NULL;
        }
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
    struct bufferevent *m_bufferEvent;
    int32_t m_authenticationResponseLength;
    AuthenticationResponse m_response;
    bool m_status;
    bool m_loginExchangeCompleted;
    int64_t m_startPending;
    ClientImpl* m_clientImpl;
};

typedef boost::shared_ptr<PendingConnection> PendingConnectionSPtr;

class CxnContext {
/*
 * Data associated with a specific connection
 */
public:
    CxnContext(const std::string& name, unsigned short port, int hostId) : m_name(name),
        m_port(port), m_nextLength(4), m_lengthOrMessage(true), m_hostId(hostId) { }
    const std::string m_name;
    const unsigned short m_port;
    int32_t m_nextLength;
    bool m_lengthOrMessage;
    int m_hostId;
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

    if (pc->m_bufferEvent != bev) {
        std::ostringstream os;
        os << "authenticationReadCallback PC buffer event: " << pc->m_bufferEvent
                << ", bev: " << bev << " " << pc->m_hostname << ":" << pc->m_port ;
        std::cerr << os.str() << std::endl;
        assert(pc->m_bufferEvent == bev);
    }
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
    AuthenticationResponse response = AuthenticationResponse(buffer);
    if (!response.success()) {
        pc->m_authenticationResponseLength =-1;
        return;
    }
    pc->m_response = response;
    pc->m_loginExchangeCompleted = true;

    bufferevent_setwatermark(bev, EV_READ, 4, HIGH_WATERMARK);
    pc->finalizeAuthentication();
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
        if (bev) {
            pc->cleanupBev();
        }
    }

    if (pc->m_startPending < 0) {
        //connection is pending from regular createConnection API
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
    ssize_t bytesRead = read(fd, buf, sizeof buf);
    (void) bytesRead;
    impl->eventBaseLoopBreak();
}

static void triggerExpiredRequestsScanCB(evutil_socket_t fd, short event, void *ctx) {
    ClientImpl *impl = reinterpret_cast<ClientImpl*>(ctx);
    impl->triggerScanForTimeoutRequestsEvent();
}

static void scanForExpiredRequestsCB(evutil_socket_t fd, short event, void *ctx) {
    ClientImpl *impl = reinterpret_cast<ClientImpl*>(ctx);
    char buf[8];
    ssize_t bytesRead = read(fd, buf, sizeof buf);
    (void) bytesRead;
    impl->purgeExpiredRequests();
}
/*
 * Only has to handle the case where there is an error or EOF
 */
static void regularEventCallback(struct bufferevent *bev, short events, void *ctx) {
    ClientImpl *impl = reinterpret_cast<ClientImpl*>(ctx);
    impl->regularEventCallback(bev, events);
}

boost::atomic<uint32_t> ClientImpl::m_numberOfClients(0);
boost::mutex ClientImpl::m_globalResourceLock;

// Initialization of OpenSSL library that gets called only once
pthread_once_t once_initOpenSSL = PTHREAD_ONCE_INIT;
void initOpenSSL() {
    SSL_library_init();
    ERR_load_crypto_strings();
    SSL_load_error_strings();
    OpenSSL_add_all_algorithms();
}

void ClientImpl::initSsl() throw (SSLException) {
    //init_ssl_locking();
    pthread_once(&once_initOpenSSL, initOpenSSL);
    {
        boost::mutex::scoped_lock(m_globalResourceLock);
        if (m_clientSslCtx == NULL) {
            m_clientSslCtx = SSL_CTX_new(SSLv23_client_method());
            if (m_clientSslCtx == NULL) {
                throw SSLException("Failed allocating ssl context using SSLv23 client method");
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
static void regularWriteCallback(struct bufferevent *bev, void *ctx) {
    ClientImpl *impl = reinterpret_cast<ClientImpl*>(ctx);
    impl->regularWriteCallback(bev);
}

ClientImpl::~ClientImpl() {
    bool cleanupEvp = false;
    bool cleanupErrorStrings = false;
    for (std::vector<struct bufferevent *>::iterator i = m_bevs.begin(); i != m_bevs.end(); ++i) {
        if (m_enableSSL) {
            notifySslClose(*i);
        }
        bufferevent_free(*i);
    }
    m_bevs.clear();
    m_contexts.clear();
    m_callbacks.clear();
    if (m_passwordHash != NULL) {
        free(m_passwordHash);
        cleanupEvp = true;
    }
    if (m_cfg != NULL) {
        event_config_free(m_cfg);
    }
    if (m_ev != NULL) {
        event_free(m_ev);
    }

    if (m_timerMonitorEventInitialized) {
        pthread_cancel(m_queryTimeoutMonitorThread);
        // cancel and free any timer events
        if (m_timerMonitorEventPtr) {
            if (evtimer_pending(m_timerMonitorEventPtr, NULL)) {
                evtimer_del(m_timerMonitorEventPtr);
            }
            event_free(m_timerMonitorEventPtr);
        }
        // free up rest of timer management trackers
        if (m_timeoutServiceEventPtr) {
            event_free(m_timeoutServiceEventPtr);
        }
        if (m_timerMonitorBase) {
            event_base_free(m_timerMonitorBase);
        }
        ::close(m_timerCheckPipe[0]);
        ::close(m_timerCheckPipe[1]);
        m_timerMonitorEventInitialized = false;
    }

    event_base_free(m_base);

    if (m_wakeupPipe[1] != -1) {
       ::close(m_wakeupPipe[0]);
       ::close(m_wakeupPipe[1]);
    }

#if 1
    // ssl ctx per client
    if (m_enableSSL) {
        if (m_clientSslCtx != NULL) {
            SSL_CTX_free(m_clientSslCtx);
            m_clientSslCtx = NULL;
        }
        cleanupErrorStrings = true;
    }
#endif

    {
        boost::mutex::scoped_lock lock(m_globalResourceLock);
        if (--m_numberOfClients == 0) {
            if (cleanupEvp) {
                // clean up/unload message digests n algos
                EVP_cleanup();
            }
            if (m_enableSSL) {
#if 0
                // if using global context between clients
                if (m_clientSslCtx != NULL) {
                    SSL_CTX_free(m_clientSslCtx);
                    m_clientSslCtx = NULL;
                }
#endif
                // unload ssl n crypto error strings
                ERR_free_strings();
            }
        }
    }
}

// Initialization for the library that only gets called once
pthread_once_t once_initLibevent = PTHREAD_ONCE_INIT;
void initLibevent() {
    // try to run threadsafe libevent
    int status = evthread_use_pthreads();
    if (status) {
        std::ostringstream msg;
        msg << "Failed during initLibevent: " << status;
        throw voltdb::LibEventException(msg.str());
    }
}

const int64_t ClientImpl::VOLT_NOTIFICATION_MAGIC_NUMBER(9223372036854775806);
const std::string ClientImpl::SERVICE("database");

class CleanupEVP_MD_Ctx{
public:
    explicit CleanupEVP_MD_Ctx(EVP_MD_CTX *ctx) : m_ctx(ctx) { }
    ~CleanupEVP_MD_Ctx() {
        if (m_ctx != NULL) {
            EVP_MD_CTX_cleanup(m_ctx);
        }
    }
private:
    EVP_MD_CTX* m_ctx;
};

void ClientImpl::hashPassword(const std::string& password) throw (MDHashException) {
    const EVP_MD *md = NULL;
    size_t hashDataLength;
    if (m_hashScheme == HASH_SHA1) {
        hashDataLength = SHA_DIGEST_LENGTH;
        md = EVP_sha1();
    }
    else if (m_hashScheme == HASH_SHA256) {
        hashDataLength = SHA256_DIGEST_LENGTH;
        md = EVP_sha256();
    }
    else {
        throw voltdb::MDHashException("Supported hash-schemes are SHA1 and SHA256");
    }
    if (md == NULL) {
        throw MDHashException("failed getting digest for " + (m_hashScheme == HASH_SHA1) ? "SHA1" : "SHA256");
    }
    unsigned int md_len = -1;
    m_passwordHash = (unsigned char *) malloc(hashDataLength);

    EVP_MD_CTX mdctx;
    EVP_MD_CTX_init(&mdctx);
    CleanupEVP_MD_Ctx cleanup(&mdctx);
    if (EVP_DigestInit_ex(&mdctx, md, NULL) == 0) {
        throw MDHashException("Failed to setup the digest");
    }
    if (EVP_DigestUpdate(&mdctx, password.c_str(), password.size()) == 0) {
        throw MDHashException("Failed to generate digest hash");
    }
    if (EVP_DigestFinal_ex(&mdctx, m_passwordHash, &md_len) == 0) {
        throw MDHashException("Failed to retrieve the digest");
    }
}

ClientImpl::ClientImpl(ClientConfig config) throw (voltdb::Exception, voltdb::LibEventException, MDHashException, SSLException) :
        m_base(NULL), m_ev(NULL), m_cfg(NULL), m_nextRequestId(INT64_MIN), m_nextConnectionIndex(0),
        m_listener(config.m_listener), m_invocationBlockedOnBackpressure(false),
        m_backPressuredForOutstandingRequests(false), m_loopBreakRequested(false),
        m_isDraining(false), m_instanceIdIsSet(false), m_outstandingRequests(0), m_leaderAddress(-1),
        m_clusterStartTime(-1), m_username(config.m_username), m_passwordHash(NULL), m_maxOutstandingRequests(config.m_maxOutstandingRequests),
        m_ignoreBackpressure(false), m_useClientAffinity(true),m_updateHashinator(false), m_enableAbandon(config.m_enableAbandon), m_pendingConnectionSize(0),
        m_enableQueryTimeout(config.m_enableQueryTimeout), m_queryTimeoutMonitorThread(0), m_timerMonitorBase(NULL), m_timerMonitorEventPtr(NULL),
        m_timeoutServiceEventPtr(NULL), m_timerMonitorEventInitialized(false), m_timedoutRequests(0), m_responseHandleNotFound(0),
        m_queryExpirationTime(config.m_queryTimeout), m_scanIntervalForTimedoutQuery(config.m_scanIntervalForTimedoutQuery),
        m_pLogger(0), m_hashScheme(config.m_hashScheme), m_enableSSL(config.m_useSSL), m_clientSslCtx(NULL) {
    pthread_once(&once_initLibevent, initLibevent);
#ifdef DEBUG
    if (!voltdb_clientimpl_debug_init_libevent) {
        event_enable_debug_mode();
        voltdb_clientimpl_debug_init_libevent = true;
    }
#endif
    m_cfg = event_config_new();
    if (m_cfg == NULL) {
        throw voltdb::LibEventException("Failed creating event config");
    }
    event_config_set_flag(m_cfg, EVENT_BASE_FLAG_NO_CACHE_TIME);//, EVENT_BASE_FLAG_NOLOCK);
    m_base = event_base_new_with_config(m_cfg);
    assert(m_base);
    if (m_base == NULL) {
        throw voltdb::LibEventException("Failed creating main base");
    }
    hashPassword(config.m_password);
    if (m_enableSSL) {
        initSsl();
    }
    m_wakeupPipe[0] = -1;
    m_wakeupPipe[1] = -1;

    if (m_enableQueryTimeout) {
        m_timerMonitorBase = event_base_new();
        if (m_timerMonitorBase == NULL) {
            throw voltdb::LibEventException("Failed creating event base for timer");
        }
    }
    m_timerCheckPipe[0] = -1;
    m_timerCheckPipe[1] = -1;

    ++m_numberOfClients;
}

class FreeBEVOnFailure {
public:
    FreeBEVOnFailure(struct bufferevent *bev) : m_pc(NULL), m_bev(bev) {}

    FreeBEVOnFailure(PendingConnection *pc) : m_pc(pc), m_bev(pc->m_bufferEvent) {}
    ~FreeBEVOnFailure() {
        if (m_bev) {
            if (m_pc) {
                m_pc->cleanupBev();
            }
            else {
                bufferevent_free(m_bev);
            }
        }
    }
    void success() {
        m_bev = NULL;
    }
private:
    PendingConnection *m_pc;
    struct bufferevent *m_bev;
};

void ClientImpl::initiateConnection(boost::shared_ptr<PendingConnection> &pc) throw (voltdb::ConnectException,
                                                                                     voltdb::LibEventException,
                                                                                     SSLException) {
    std::ostringstream ss;
    ss << "ClientImpl::initiateConnection to " << pc->m_hostname << ":" << pc->m_port;

    if (pc->m_bufferEvent != NULL) {
        ss << ", clean up existing bev: " << pc->m_bufferEvent;
        pc->cleanupBev();
    }

    if (m_enableSSL) {
        SSL *bevSsl = SSL_new(m_clientSslCtx);
        if (bevSsl == NULL) {
            ss.str("");
            ss << "failed allocating SSL structure for connection: " << pc->m_hostname << ":" << pc->m_port;
            throw SSLException(ss.str());
        }
        pc->m_bufferEvent = bufferevent_openssl_socket_new(m_base, -1, bevSsl, BUFFEREVENT_SSL_CONNECTING,
                BEV_OPT_CLOSE_ON_FREE | BEV_OPT_THREADSAFE);
        bufferevent_openssl_get_allow_dirty_shutdown(pc->m_bufferEvent);
    }
    else {
        pc->m_bufferEvent = bufferevent_socket_new(m_base, -1, BEV_OPT_CLOSE_ON_FREE | BEV_OPT_THREADSAFE);
    }
    if (pc->m_bufferEvent == NULL) {
        if (pc->m_keepConnecting) {
            createPendingConnection(pc->m_hostname, pc->m_port);
        } else {
            ss.str("");
            ss << "!!!! ClientImpl::initiateConnection to " << pc->m_hostname << ":" << pc->m_port << " failed getting socket";
            logMessage(ClientLogger::ERROR, ss.str());

            throw ConnectException(pc->m_hostname, pc->m_port);
        }
    }
    ss << ", new bev: " <<  pc->m_bufferEvent;
    logMessage(ClientLogger::INFO, ss.str());
    FreeBEVOnFailure protector(pc.get());
    bufferevent_setcb(pc->m_bufferEvent, authenticationReadCallback, NULL, authenticationEventCallback, pc.get());
    //std::cout << ss.str() << " thread-id: " << (long) pthread_self() << std::endl;

    if (bufferevent_socket_connect_hostname(pc->m_bufferEvent, NULL, AF_INET, pc->m_hostname.c_str(), pc->m_port) != 0) {
        if (pc->m_keepConnecting) {
            //std::cout << "CI::free bev: " << pc->m_bufevent <<std::endl;
            if (pc->m_bufferEvent != NULL) {
                pc->cleanupBev();
            }
            protector.success();
            createPendingConnection(pc->m_hostname, pc->m_port);
        } else {
            ss.str("");
            ss << "!!!! ClientImpl::initiateConnection to " << pc->m_hostname << ":" << pc->m_port << " failed";
            logMessage(ClientLogger::ERROR, ss.str());

            throw voltdb::LibEventException(ss.str());
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
    for (std::vector<struct bufferevent *>::iterator bevEntryItr = m_bevs.begin(); bevEntryItr != m_bevs.end(); ++bevEntryItr) {
        if (m_enableSSL) {
            notifySslClose(*bevEntryItr);
        }
        bufferevent_free(*bevEntryItr);
    }
    m_bevs.clear();
}

void ClientImpl::initiateAuthentication(struct bufferevent *bev, const std::string& hostname, unsigned short port) throw (voltdb::LibEventException) {

    logMessage(ClientLogger::DEBUG, "ClientImpl::initiateAuthentication");

    FreeBEVOnFailure protector(bev);
    bufferevent_setwatermark( bev, EV_READ, 4, HIGH_WATERMARK);
    bufferevent_setwatermark( bev, EV_WRITE, 8192, 262144);

    if (bufferevent_enable(bev, EV_READ)) {
        std::ostringstream os;
        os << "initiateAuthentication: failed to enable read events "<< hostname << ":" << (unsigned int) port << "; bev:" << bev;
        if (m_pLogger) {
            m_pLogger->log(ClientLogger::ERROR, os.str());
        } else {
            std::cerr << os.str() << std::endl;
        }
        throw voltdb::LibEventException(os.str());
    }
    AuthenticationRequest authRequest(m_username, SERVICE, m_passwordHash, m_hashScheme );
    ScopedByteBuffer bb(authRequest.getSerializedSize());
    authRequest.serializeTo(&bb);

    struct evbuffer *evbuf = bufferevent_get_output(bev);
    if (evbuffer_add( evbuf, bb.bytes(), static_cast<size_t>(bb.remaining()))) {
        std::ostringstream os;
        os << "initiateAuthentication: failed to add data event buffer"<< hostname << ":" << (unsigned int) port << "; bev:" << bev;
        if (m_pLogger) {
            m_pLogger->log(ClientLogger::ERROR, os.str());
        } else {
            std::cerr << os.str() <<std::endl;
        }
        throw voltdb::LibEventException(os.str());
    }
    protector.success();
}

void ClientImpl::finalizeAuthentication(PendingConnection* pc) throw (voltdb::Exception,
                                                                      voltdb::ConnectException) {

    logMessage(ClientLogger::DEBUG, "ClientImpl::finalizeAuthentication");

    FreeBEVOnFailure protector(pc);
    bool pcRemoved = false;
    bool exitEventLoop = false;
    struct bufferevent *bev = pc->m_bufferEvent;

    event_base *evBasePtr = pc->m_base;
    if (pc->m_startPending < 0) {
        // triggered through create connection
        exitEventLoop = true;
    }

    if (pc->m_loginExchangeCompleted) {

        logMessage(ClientLogger::DEBUG, "ClientImpl::finalizeAuthentication OK");

        if (!m_instanceIdIsSet) {
            m_instanceIdIsSet = true;
            m_clusterStartTime = pc->m_response.getClusterStartTime();
            m_leaderAddress = pc->m_response.getLeaderAddress();
        } else {
            if (m_clusterStartTime != pc->m_response.getClusterStartTime() ||
                m_leaderAddress != pc->m_response.getLeaderAddress()) {
                throw ClusterInstanceMismatchException();
            }
        }
        //save event for host id
        int hostId = pc->m_response.getHostId();
        m_hostIdToEvent[hostId] = bev;
        bufferevent_setwatermark( bev, EV_READ, 4, HIGH_WATERMARK);
        m_bevs.push_back(bev);

        // save connection information for the event
        m_contexts[bev] =
               boost::shared_ptr<CxnContext>(new CxnContext(pc->m_hostname, pc->m_port, hostId));
        boost::shared_ptr<CallbackMap> callbackMap(new CallbackMap());
        m_callbacks[bev] = callbackMap;

        pc->m_bufferEvent = NULL;
        bufferevent_setcb(bev,
                          voltdb::regularReadCallback,
                          voltdb::regularWriteCallback,
                          voltdb::regularEventCallback,
                          this);

        {
            boost::mutex::scoped_lock lock(m_pendingConnectionLock);
            for (std::list<PendingConnectionSPtr>::iterator i = m_pendingConnectionList.begin();
                 i != m_pendingConnectionList.end();
                 ++i) {
                if (i->get() == pc) {
                    m_pendingConnectionList.erase(i);
                    m_pendingConnectionSize.store(m_pendingConnectionList.size(), boost::memory_order_release);
                    pcRemoved = true;
                    break;
                }
            }
        }

        //update topology info and procedures info
        if (m_useClientAffinity) {
            updateHashinator();
            subscribeToTopologyNotifications();
        }

        std::ostringstream ss;
        ss << "connectionActive " << m_contexts[bev]->m_name << ":" << m_contexts[bev]->m_port ;
        logMessage(ClientLogger::INFO, ss.str());

        //Notify client that a connection was active
        if (m_listener.get() != NULL) {
            try {
                 m_listener->connectionActive( m_contexts[bev]->m_name, m_bevs.size() );
            } catch (const std::exception& e) {
                ss.str("");
                ss << "Exception while reporting connection active to status listener: " << e.what() << std::endl;
                logMessage(ClientLogger::ERROR, ss.str());
            }
        }

        // set up timer thread if query timeout is enabled
        if (m_timerMonitorEventInitialized == false && m_enableQueryTimeout) {
            assert(m_timerCheckPipe[0] == -1);
            assert(m_timerCheckPipe[1] == -1);

            if (pipe(m_timerCheckPipe) == 0) {
                setUpTimeoutCheckerMonitor();
                m_timerMonitorEventInitialized = true;
            }
            else {
                throw PipeCreationException();
            }
        }
    }
    else {
        logMessage(ClientLogger::DEBUG, "ClientImpl::finalizeAuthentication Fail");

        std::ostringstream ss;
        ss << "connection failed " << " " << pc->m_hostname << ":" << pc->m_port;
        logMessage(ClientLogger::ERROR, ss.str());

        throw ConnectException();
    }
    if (exitEventLoop) {
        event_base_loopexit(evBasePtr, NULL);
    }
    else if (!pcRemoved) {
        logMessage(ClientLogger::INFO, "ClientImpl::finalizeAuthentication, update start pending time");
        pc->m_startPending = get_sec_time();
    }
    protector.success();
}

void *timerThreadRun(void *ctx) {
    ClientImpl *client = reinterpret_cast<ClientImpl*>(ctx);
    client->runTimeoutMonitor();
}

void ClientImpl::runTimeoutMonitor() throw (voltdb::LibEventException) {
    if (event_base_dispatch(m_timerMonitorBase) == -1) {
        throw LibEventException("runTimeoutMonitor: failed running event loop for timer monitor");
    } else {
        //std::cout << "dispatched timer event: " << std::endl;
    }
}

void ClientImpl::startMonitorThread() throw (voltdb::TimerThreadException){
    pthread_attr_t threadAttr;
    pthread_attr_init(&threadAttr);
    pthread_attr_setdetachstate(&threadAttr, PTHREAD_CREATE_DETACHED);
    int status = pthread_create(&m_queryTimeoutMonitorThread, &threadAttr, timerThreadRun, this);
    pthread_attr_destroy(&threadAttr);
    if (status != 0) {
        std::ostringstream os;
        os << "startMonitorThread: Thread creation failed, failure code: " << status;
        throw new voltdb::TimerThreadException(os.str());
    }
}

void ClientImpl::setUpTimeoutCheckerMonitor() throw (voltdb::LibEventException){
    // setup to receive timeout scan notification
    m_timeoutServiceEventPtr = event_new(m_base, m_timerCheckPipe[0], EV_READ | EV_PERSIST, scanForExpiredRequestsCB, this);
    if (m_timeoutServiceEventPtr == NULL) {
        throw LibEventException("setUpTimeoutCheckerMonitor: failed creating timer event");
    }
    event_add(m_timeoutServiceEventPtr, NULL);

    // setup to send timeout scan notification
    m_timerMonitorEventPtr = evtimer_new(m_timerMonitorBase, triggerExpiredRequestsScanCB, this);
    if (m_timerMonitorEventPtr == NULL) {
        throw LibEventException("setUpTimeoutCheckerMonitor: evtimer_new failed");
    }
    if (evtimer_add(m_timerMonitorEventPtr, &m_scanIntervalForTimedoutQuery) != 0) {
        throw LibEventException("setUpTimeoutCheckerMonitor: failed adding timeout event");
    }
    startMonitorThread();
}

void ClientImpl::createConnection(const std::string& hostname,
                                  const unsigned short port,
                                  const bool keepConnecting) throw (voltdb::Exception,
                                                                    voltdb::ConnectException,
                                                                    voltdb::LibEventException,
                                                                    voltdb::PipeCreationException,
                                                                    voltdb::TimerThreadException,
                                                                    SSLException) {


    if (m_pLogger) {
        std::ostringstream os;
        os << "ClientImpl::createConnection" << " hostname:" << hostname << " port:" << port;
        m_pLogger->log(ClientLogger::INFO, os.str());
    }

    if (0 == pipe(m_wakeupPipe)) {
        if (m_ev != NULL) {
            event_free(m_ev);
        }
        m_ev = event_new(m_base, m_wakeupPipe[0], EV_READ|EV_PERSIST, wakeupPipeCallback, this);
        event_add(m_ev, NULL);
    } else {
        m_wakeupPipe[1] = -1;
    }

    PendingConnectionSPtr pc(new PendingConnection(hostname, port, keepConnecting, m_base, this));
    initiateConnection(pc);

    int dispatchStatus = event_base_dispatch(m_base);
    if (dispatchStatus == -1) {
        throw voltdb::LibEventException("CreateConnection: Failed running base loop");
    }

    if (pc->m_status) {
        dispatchStatus = event_base_dispatch(m_base);
        if (dispatchStatus == -1) {
            throw voltdb::LibEventException("CreateConnection: Failed running base loop");
        }

        if (pc->m_loginExchangeCompleted) {
            return;
        }
    }
    if (keepConnecting) {
        if (pc->m_bufferEvent != NULL)  {
            pc->cleanupBev();
        }
        createPendingConnection(hostname, port);
    } else {
        // if no error has been reported for the connection, back off and listen if
        // any events were there to process before calling it no-connection
        int retry = 0;
        while (pc->m_status && retry < 5) {
            ++retry;
            dispatchStatus = event_base_dispatch(m_base);
            if (dispatchStatus == -1) {
                throw voltdb::LibEventException("CreateConnection: Failed running base loop");
            }
            timespec ts;
            ts.tv_sec = 0;
            ts.tv_nsec = 10000;
            nanosleep(&ts, NULL);
            if (pc->m_loginExchangeCompleted) {
                return;
            }
        }
        throw ConnectException(hostname, port);
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
        if ((now - pc->m_startPending) > RECONNECT_INTERVAL) {
            pc->m_startPending = now;
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
    tv.tv_sec = (time > 0)? RECONNECT_INTERVAL : 0;
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

void ClientImpl::purgeExpiredRequests() {
    struct timeval now;
    event_base_gettimeofday_cached(m_base, &now);
    BEVToCallbackMap::iterator end = m_callbacks.end();

    std::vector<voltdb::Table> dummyTable;
    InvocationResponse response(0, voltdb::STATUS_CODE_CONNECTION_TIMEOUT, "client timedout waiting for response",
            voltdb::STATUS_CODE_UNINITIALIZED_APP_STATUS_CODE, "No response received in allotted time",
            dummyTable);

    for (BEVToCallbackMap::iterator itr = m_callbacks.begin(); itr != end; ++itr) {
        boost::shared_ptr<CallbackMap> callbackMap = itr->second;
        for (CallbackMap::iterator cbItr =  callbackMap->begin();
                cbItr != callbackMap->end(); ++cbItr) {
            timeval expirationTime = cbItr->second->getExpirationTime();

            if (cbItr->second->isReadOnly() && (!timercmp(&expirationTime, &now, >))) {
                response.setClientData(cbItr->first);
                try {
                    cbItr->second->getCallback()->callback(response);
                } catch (std::exception &excp) {
                    if (m_listener.get() != NULL) {
                        try {
                            m_listener->uncaughtException(excp, cbItr->second->getCallback(), response);
                        } catch (const std::exception &e) {
                            std::string str ("Uncaught exception");
                            logMessage(ClientLogger::ERROR, str + e.what());
                        }
                    }
                }
                callbackMap->erase(cbItr);
                --m_outstandingRequests;
                ++m_timedoutRequests;
            }
        }
    }
}

InvocationResponse ClientImpl::invoke(Procedure &proc) throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::UninitializedParamsException, voltdb::LibEventException) {

    // Before making a synchronous request, process any existing requests.
    while (! drain()) {}

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
        throw voltdb::LibEventException("Synchronous invoke: failed adding data to event buffer");
    }
    timeval tv, expirationTime;
    int status = gettimeofday(&tv, NULL);
    assert(status == 0);
    expirationTime.tv_sec = tv.tv_sec + m_queryExpirationTime.tv_sec;
    expirationTime.tv_usec = tv.tv_usec + m_queryExpirationTime.tv_usec;
    boost::shared_ptr<CallBackBookeeping> cb (new CallBackBookeeping(callback, expirationTime));
    m_outstandingRequests++;
    (*m_callbacks[bev])[clientData] = cb;

    if (event_base_dispatch(m_base) == -1) {
        throw voltdb::LibEventException("Synchronous invoke: failed running base loop");
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

    bool allowAbandon() const {
        return m_callback->allowAbandon();
    }

};

void ClientImpl::invoke(Procedure &proc, ProcedureCallback *callback) throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::UninitializedParamsException, voltdb::LibEventException, voltdb::ElasticModeMismatchException) {
    boost::shared_ptr<ProcedureCallback> wrapper(new DummyCallback(callback));
    invoke(proc, wrapper);
}

bool ClientImpl::isReadOnly(const Procedure &proc) {
    ProcedureInfo *procInfo = m_distributer.getProcedure(proc.getName());
    return (procInfo != NULL && procInfo->m_readOnly);
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
        std::map<int, bufferevent*>::iterator bevEntry = m_hostIdToEvent.find(hostId);
        if (bevEntry != m_hostIdToEvent.end()) {
            return bevEntry->second;
        }
    }
    return NULL;
}


void ClientImpl::invoke(Procedure &proc, boost::shared_ptr<ProcedureCallback> callback) throw (voltdb::Exception,
                                                                                               voltdb::NoConnectionsException,
                                                                                               voltdb::UninitializedParamsException,
                                                                                               voltdb::LibEventException,
                                                                                               voltdb::ElasticModeMismatchException) {
    if (callback.get() == NULL) {
        throw voltdb::NullPointerException();
    }
    if (m_bevs.empty()) {
        throw voltdb::NoConnectionsException();
    }

    if (m_outstandingRequests >= m_maxOutstandingRequests) {
    	if (m_listener.get() != NULL) {
            m_backPressuredForOutstandingRequests = true;
            try {
                m_listener->backpressure(true);
            } catch (const std::exception& e) {
                std::cerr << "Exception thrown on invocation of backpressure callback: " << e.what() << std::endl;
            }
        }
    	// We are overloaded, we need to reject traffic and notify the caller
        if (m_enableAbandon && callback->allowAbandon()) {
            callback->abandon(ProcedureCallback::TOO_BUSY);
            return;
        }
        else if (m_enableAbandon) {
            // report to client the request was not abandoned
            callback->abandon(ProcedureCallback::NOT_ABANDONED);
        }
    }

    //do not call the procedures if hashinator is in the LEGACY mode
    if (!m_distributer.isUpdating() && !m_distributer.isElastic()) {
        //todo: need to remove the connection
        throw voltdb::ElasticModeMismatchException();
    }

    timeval entryTime, expirationTime;
    // optimization? - small cost benefit with using clock_gettime( monotonic)at expense of preciseness of time
    int status = gettimeofday(&entryTime, NULL);
    assert(status == 0);
    expirationTime.tv_sec = entryTime.tv_sec + m_queryExpirationTime.tv_sec;
    expirationTime.tv_usec = entryTime.tv_usec + m_queryExpirationTime.tv_usec;

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
                    std::string msg(e.what());
                    logMessage(ClientLogger::ERROR, "Exception thrown on invocation of backpressure callback: " + msg);
                }
            }
            if (callEventLoop) {
                m_invocationBlockedOnBackpressure = true;
                int loopRunStatus = event_base_dispatch(m_base);
                if (loopRunStatus == -1) {
                    std::string msg("invoke: failed running event base loop to ease out backpressure");
                    if (m_pLogger) {
                        m_pLogger->log(ClientLogger::ERROR, msg);
                    } else {
                        std::cerr << msg << std::endl;
                    }
                    throw voltdb::LibEventException(msg);
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

    bool procReadOnly = false;
    struct bufferevent *routed_bev = NULL;
    //route transaction to correct event if client affinity is enabled and hashinator updating is not in progress
    //elastic scalability is disabled
    if (m_useClientAffinity && !m_distributer.isUpdating()) {
         routed_bev = routeProcedure(proc, sbb);
        // Check if the routed_bev is valid and has not been removed due to lost connection
        if ((routed_bev != NULL) && (m_callbacks.find(routed_bev) != m_callbacks.end())) {
            bev = routed_bev;
        }

        if (isReadOnly(proc)) {
            procReadOnly = true;
        }
    }

    CallBackBookeeping *cbPtr = new CallBackBookeeping(callback, expirationTime, procReadOnly);
    assert (cbPtr != NULL);
    boost::shared_ptr<CallBackBookeeping> cb (cbPtr);

    BEVToCallbackMap::iterator bevFromCBMap = m_callbacks.find(bev);
    if ( bevFromCBMap == m_callbacks.end()) {
        //std::cerr << "invoke::No connection error" << std::endl;
        throw NoConnectionsException();
    }
    (*(bevFromCBMap->second))[clientData] = cb;

    //(*(m_callbacks[bev]))[clientData] = cb;
    ++m_outstandingRequests;

    struct evbuffer *evbuf = bufferevent_get_output(bev);
    if (evbuffer_add(evbuf, sbb.bytes(), static_cast<size_t>(sbb.remaining()))) {
        throw voltdb::LibEventException("invoke: Failed adding data to event buffer");
    }

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
        throw voltdb::LibEventException("runOnce: failed running event base loop");
    }
    m_loopBreakRequested = false;
}

void ClientImpl::run() throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::LibEventException) {

    logMessage(ClientLogger::DEBUG, "ClientImpl::run");

    if (m_bevs.empty() && m_pendingConnectionSize.load(boost::memory_order_consume) <= 0) {
        throw voltdb::NoConnectionsException();
    }
    if (event_base_dispatch(m_base) == -1) {
        throw voltdb::LibEventException("run: Failed running event base loop");
    }
    m_loopBreakRequested = false;
}

void ClientImpl::regularReadCallback(struct bufferevent *bev) {
    struct evbuffer *evbuf = bufferevent_get_input(bev);
    // todo: for debug only
    assert(m_contexts.find(bev) != m_contexts.end());
    boost::shared_ptr<CxnContext> context = m_contexts[bev];
    int32_t remaining = static_cast<int32_t>(evbuffer_get_length(evbuf));
    if (context->m_lengthOrMessage && remaining < 4) {
        return;
    }

    bool breakEventLoop = false;
    while (true) {
        if (context->m_lengthOrMessage && (remaining >= 4)) {
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
            int64_t clientData = response.clientData();

            if (clientData == VOLT_NOTIFICATION_MAGIC_NUMBER) {
                if (!response.failure()){
                    m_distributer.handleTopologyNotification(response.results());
                }
            } else {
                BEVToCallbackMap::iterator entry = m_callbacks.find(bev);
                if (entry != m_callbacks.end()) {
                    CallbackMap::iterator cbMapIterator = entry->second->find(clientData);
                    if (cbMapIterator != entry->second->end()) {
                        try {
                            m_ignoreBackpressure = true;
                            breakEventLoop |= cbMapIterator->second->getCallback()->callback(response);
                            m_ignoreBackpressure = false;
                        } catch (const std::exception &e) {
                            if (m_listener.get() != NULL) {
                                try {
                                    m_ignoreBackpressure = true;
                                    breakEventLoop |= m_listener->uncaughtException( e, cbMapIterator->second->getCallback(), response);
                                    m_ignoreBackpressure = false;
                                } catch (const std::exception& e) {
                                    std::string reason(e.what());
                                    logMessage( ClientLogger::ERROR, "Uncaught exception handler threw exception: " + reason);
                                }
                            }
                        }
                        entry->second->erase(cbMapIterator);
                        --m_outstandingRequests;
                    }
                    else {
                        ++m_responseHandleNotFound;
                    }
                }
                else {
                    //std::cout << "bev entry not detected: "<< bev << std::endl;
                }
            }

            //If the client is draining and it just drained the last request, break the loop
            if (m_isDraining && (m_outstandingRequests == 0)) {
                m_isDraining = false;
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

    if ((m_outstandingRequests < m_maxOutstandingRequests) && m_backPressuredForOutstandingRequests) {
        if (m_listener.get() != NULL) {
            m_backPressuredForOutstandingRequests = false;
            try {
                m_listener->backpressure(false);
            }
            catch (const std::exception &excp) {
                std::string reason(excp.what());
                logMessage(ClientLogger::ERROR, "Caught exception when notifying for backpressure gone: " + reason);
            }
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

        if (m_enableSSL) {
            // check if there are wrapper in evutil for ssl errors
            unsigned long sslErr = ERR_get_error();
            const char* const errMsg = ERR_reason_error_string(sslErr);
            if (errMsg) {
                std::string msg = "SSL error string: "+ std::string(errMsg);
                logMessage(ClientLogger::ERROR, msg);
            }
        }

        std::map<struct bufferevent *, boost::shared_ptr<CxnContext> >::iterator connectionCtxIter = m_contexts.find(bev);
        assert(connectionCtxIter != m_contexts.end());
        // First drain anything in the read buffer
        regularReadCallback(bev);

        bool breakEventLoop = false;

        if (m_pLogger) {
            std::ostringstream os;
            const char* s_error = (events & BEV_EVENT_ERROR) ? "BEV_EVENT_ERROR" :"BEV_EVENT_EOF";
            os << "connectionLost: " << s_error;
            logMessage(ClientLogger::ERROR, os.str());
        }

        //Notify client that a connection was lost
        if (m_listener.get() != NULL) {
            try {
                m_ignoreBackpressure = true;
                breakEventLoop |= m_listener->connectionLost( connectionCtxIter->second->m_name, m_bevs.size() - 1);
                m_ignoreBackpressure = false;
            } catch (const std::exception& e) {
                std::string msg(e.what());
                logMessage(ClientLogger::ERROR, "Status listener threw exception on connection lost: " + msg);
            }
        }
        // Iterate the list of callbacks for this connection and invoke them
        // with the appropriate error response
        BEVToCallbackMap::iterator callbackMapIter = m_callbacks.find(bev);
        if (callbackMapIter != m_callbacks.end()) {
            boost::shared_ptr<CallbackMap> callbackMap = callbackMapIter->second;
            InvocationResponse response = InvocationResponse();
            for (CallbackMap::iterator i =  callbackMap->begin();
                    i != callbackMap->end(); ++i) {
                try {
                    breakEventLoop |=
                            i->second->getCallback()->callback(response);
                } catch (std::exception &e) {
                    if (m_listener.get() != NULL) {
                        breakEventLoop |= m_listener->uncaughtException( e, i->second->getCallback(), response);
                    }
                }
                --m_outstandingRequests;
            }
            //Remove the callback map from the callbacks map
            m_callbacks.erase(callbackMapIter);
        }

        if (m_isDraining && (m_outstandingRequests == 0)) {
            m_isDraining = false;
            breakEventLoop = true;
        }

        m_hostIdToEvent.erase(connectionCtxIter->second->m_hostId);

        //remove the entry for the backpressured connection set
        m_backpressuredBevs.erase(bev);
        if (m_outstandingRequests < m_maxOutstandingRequests) {
            m_backPressuredForOutstandingRequests = false;
        }

        createPendingConnection(connectionCtxIter->second->m_name, connectionCtxIter->second->m_port, get_sec_time());

        //Remove the connection context
        m_contexts.erase(bev);

        std::vector<bufferevent *>::iterator entry = std::find(m_bevs.begin(), m_bevs.end(), bev);
        if (entry != m_bevs.end()) {
            m_bevs.erase(entry);
        }

        bufferevent_free(bev);
        //Reset cluster Id as no more connections left
        if (m_bevs.empty()) {
            m_instanceIdIsSet = false;
        }

        if (breakEventLoop || (m_bevs.size() == 0)) {
            event_base_loopbreak( m_base );
        }

        //update topology info and procedures info
        if (m_useClientAffinity && (m_bevs.size() > 0)) {
            updateHashinator();
        }
    }
}

void ClientImpl::regularWriteCallback(struct bufferevent *bev) {
    std::set<struct bufferevent*>::iterator bpBevIterator =
            m_backpressuredBevs.find(bev);
    if (bpBevIterator != m_backpressuredBevs.end()) {
        m_backpressuredBevs.erase(bpBevIterator);
        if (m_listener.get() != NULL) {
            try {
                m_listener->backpressure(false);
            } catch (const std::exception& excp) {
                std::string msg(excp.what());
                logMessage(ClientLogger::ERROR,  "Caught exception while reporting backpressure off. " + msg);
            }
        }
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
    TopoUpdateCallback(Distributer *dist, ClientLogger *logger) : m_dist(dist), m_logger(logger) {}

    bool callback(InvocationResponse response) throw (voltdb::Exception)
    {
        if (response.failure()){
            if (m_logger) {
                std::string msg = "Failure response TopoUpdateCallback::callback: " + response.statusString();
                m_logger->log(ClientLogger::ERROR, msg);
            }
            else {
                //std::cerr << os.str() << std::endl;
            }
            return false;
        }
        //std::cout << "Update topology!" <<std::endl;
        m_dist->updateAffinityTopology(response.results());
        return true;
    }

    bool allowAbandon() const {return false;}

 private:
    Distributer *m_dist;
    ClientLogger *m_logger;
};

class SubscribeCallback : public voltdb::ProcedureCallback
{
public:
    SubscribeCallback(ClientLogger *logger) : m_logger(logger) {}

    bool callback(InvocationResponse response) throw (voltdb::Exception)
    {
        if (response.failure()) {
            if (m_logger) {
                std::string msg = "Failure response SubscribeCallback::callback: " + response.statusString();
                m_logger->log(ClientLogger::ERROR, msg);
            }
            else {
                //std::cerr  << os.str()<< std::endl;
            }
            return false;
        }
        return true;
    }

    bool allowAbandon() const {return false;}
private:
    ClientLogger *m_logger;
};

/*
 * Callback for ("@SystemCatalog", "PROCEDURES")
 */
class ProcUpdateCallback : public voltdb::ProcedureCallback
{
public:
    ProcUpdateCallback(Distributer *dist, ClientLogger *logger) : m_dist(dist), m_logger(logger) {}

    bool callback(InvocationResponse response) throw (voltdb::Exception)
    {
        if (response.failure()) {

            if (m_logger) {
                std::string msg = "Failure response SubscribeCallback::callback: " + response.statusString();
                m_logger->log(ClientLogger::ERROR, msg);
            }
            else {
                //std::cerr << os.str() << std::endl;
            }
            return false;
        }
        //std::cout << "Update ProInfo!" << std::endl;
        m_dist->updateProcedurePartitioning(response.results());
        return true;
    }

    bool allowAbandon() const {return false;}

 private:
    Distributer *m_dist;
    ClientLogger *m_logger;
};



void ClientImpl::updateHashinator(){
    m_distributer.startUpdate();
    std::vector<voltdb::Parameter> parameterTypes(1);
    parameterTypes[0] = voltdb::Parameter(voltdb::WIRE_TYPE_STRING);
    voltdb::Procedure systemCatalogProc("@SystemCatalog", parameterTypes);
    voltdb::ParameterSet* params = systemCatalogProc.params();
    params->addString("PROCEDURES");

    boost::shared_ptr<ProcUpdateCallback> procCallback(new ProcUpdateCallback(&m_distributer, m_pLogger));
    invoke(systemCatalogProc, procCallback);

    parameterTypes.resize(2);
    parameterTypes[0] = voltdb::Parameter(voltdb::WIRE_TYPE_STRING);
    parameterTypes[1] = voltdb::Parameter(voltdb::WIRE_TYPE_INTEGER);

    voltdb::Procedure statisticsProc("@Statistics", parameterTypes);
    params = statisticsProc.params();
    params->addString("TOPO").addInt32(0);

    boost::shared_ptr<TopoUpdateCallback> topoCallback(new TopoUpdateCallback(&m_distributer, m_pLogger));

    invoke(statisticsProc, topoCallback);
}

void ClientImpl::subscribeToTopologyNotifications(){
    std::vector<voltdb::Parameter> parameterTypes(1);
    parameterTypes[0] = voltdb::Parameter(voltdb::WIRE_TYPE_STRING);
    //parameterTypes[1] = voltdb::Parameter(voltdb::WIRE_TYPE_INTEGER);
    voltdb::Procedure statisticsProc("@Subscribe", parameterTypes);
    voltdb::ParameterSet* params = statisticsProc.params();
    params->addString("TOPOLOGY");

    boost::shared_ptr<SubscribeCallback> topoCallback(new SubscribeCallback(m_pLogger));

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
        if (lock) {
            ssize_t bytesWrote = write(m_wakeupPipe[1], &c, 1);
            (void) bytesWrote;
        }
    } else {
      event_base_loopbreak(m_base);
    }
}

void ClientImpl::logMessage(ClientLogger::CLIENT_LOG_LEVEL severity, const std::string& msg){
    if( m_pLogger ){
        m_pLogger->log(severity, msg);
    }
}

void ClientImpl::triggerScanForTimeoutRequestsEvent() {
    static unsigned char c = 'w';
    ssize_t bytes = write(m_timerCheckPipe[1], &c, 1);
    (void) bytes;
    int status = evtimer_add(m_timerMonitorEventPtr, &m_queryExpirationTime);
    if (status != 0) {
        std::ostringstream os;
        os << "Failed to add timer event: " << status;
        if (m_pLogger) {
            logMessage(ClientLogger::ERROR, os.str());
        }
    }
}
}
