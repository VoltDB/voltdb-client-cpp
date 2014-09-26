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

#ifndef VOLTDB_CLIENTIMPL_H_
#define VOLTDB_CLIENTIMPL_H_
#include <event2/event.h>
#include <event2/bufferevent.h>
#include <map>
#include <set>
#include <list>
#include <string>
#include "ProcedureCallback.hpp"
//#include "StatusListener.h"
#include "Client.h"
#include "Procedure.hpp"
#include <boost/atomic.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include "ClientConfig.h"
#include "Distributer.h"
namespace voltdb {

class CxnContext;
class MockVoltDB;
class Client;
class PendingConnection;

class ClientImpl {
    friend class MockVoltDB;
    friend class PendingConnection;
    friend class Client;

public:

    /*
     * Map from client data to the appropriate callback for a specific connection
     */
    typedef std::map< int64_t, boost::shared_ptr<ProcedureCallback> > CallbackMap;

    /*
     * Map from buffer event (connection) to the connection's callback map
     */
    typedef std::map< struct bufferevent*, boost::shared_ptr<CallbackMap> > BEVToCallbackMap;


    /*
     * Create a connection to the VoltDB process running at the specified host authenticating
     * using the username and password provided when this client was constructed
     * @param hostname Hostname or IP address to connect to
     * @param port Port to connect to
     * @throws voltdb::ConnectException An error occurs connecting or authenticating
     * @throws voltdb::LibEventException libevent returns an error code
     */
    void createConnection(const std::string &hostname, const unsigned short port) throw (voltdb::Exception, voltdb::ConnectException, voltdb::LibEventException);

    /*
     * Creates a pending connection that is handled in the reconnect callback
     * @param hostname Hostname or IP address to connect to
     * @param port Port to connect to
     * @param time since when connection is down
     */
    void createPendingConnection(const std::string &hostname, const unsigned short port, const int64_t time=0);
    void erasePendingConnection(PendingConnection *);

    /*
     * Synchronously invoke a stored procedure and return a the response.
     */
    InvocationResponse invoke(Procedure &proc) throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::UninitializedParamsException, voltdb::LibEventException);
    void invoke(Procedure &proc, boost::shared_ptr<ProcedureCallback> callback) throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::UninitializedParamsException, voltdb::LibEventException, voltdb::ElasticModeMismatchException);
    void invoke(Procedure &proc, ProcedureCallback *callback) throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::UninitializedParamsException, voltdb::LibEventException, voltdb::ElasticModeMismatchException);
    void runOnce() throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::LibEventException);
    void run() throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::LibEventException);

   /*
    * Enter the event loop and process pending events until all responses have been received and then return.
    * It is possible for drain to exit without having received all responses if a callback requests that the event
    * loop break.
    * @throws NoConnectionsException No connections to the database so there is no work to be done
    * @throws LibEventException An unknown error occured in libevent
    * @return true if all requests were drained and false otherwise
    */
    bool drain() throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::LibEventException);
    ~ClientImpl();

    void regularReadCallback(struct bufferevent *bev);
    void regularEventCallback(struct bufferevent *bev, short events);
    void regularWriteCallback(struct bufferevent *bev);
    void eventBaseLoopBreak();
    void reconnectEventCallback();

    /*
     * If one of the run family of methods is running on another thread, this
     * method will instruct it to exit as soon as it finishes it's current 
     * immediate task. If the thread in the run method is blocked/idle, then
     * it will return immediately.
     * The difference from the interrupt is it stops only currently running loop,
     * and has no effect if the loop isn,t running
     */
    void wakeup();

    /*
     * If one of the run family of methods is running on another thread, this
     * method will instruct it to exit as soon as it finishes it's current 
     * immediate task. If the thread in the run method is blocked/idle, then
     * it will return immediately.
     */
    void interrupt();

    /*
     * API to be called to enable client affinity (transaction homing)
     */
    void setClientAffinity(bool enable);
    bool getClientAffinity(){return m_useClientAffinity;}

    int32_t outstandingRequests() const {return m_outstandingRequests;}
    
    void setLoggerCallback(ClientLogger *pLogger) { m_pLogger = pLogger;}

private:
    ClientImpl(ClientConfig config) throw(voltdb::Exception, voltdb::LibEventException);

    void initiateAuthentication(PendingConnection* pc, struct bufferevent *bev) throw (voltdb::LibEventException);
    void finalizeAuthentication(PendingConnection* pc, struct bufferevent *bev) throw (voltdb::Exception, voltdb::ConnectException);

    /*
     * Updates procedures and topology information for transaction routing algorithm
     */
    void updateHashinator();

    /*
     * Get the buffered event based on transaction routing algorithm
     */
    struct bufferevent *routeProcedure(Procedure &proc, ScopedByteBuffer &sbb);

    /*
     * Initiate connection based on pending connection instance
     */
    void initiateConnection(boost::shared_ptr<PendingConnection> &pc) throw (voltdb::ConnectException, voltdb::LibEventException);

    /*
     * Method for sinking messages.
     * If a logger callback is not set then skip all messages
     */
    void logMessage(ClientLogger::CLIENT_LOG_LEVEL severity, const std::string& msg);

    Distributer  m_distributer;
    struct event_base *m_base;
    int64_t m_nextRequestId;
    size_t m_nextConnectionIndex;
    std::vector<struct bufferevent*> m_bevs;
    std::map<struct bufferevent *, boost::shared_ptr<CxnContext> > m_contexts;
    std::map<int, struct bufferevent *> m_hostIdToEvent;
    std::set<struct bufferevent *> m_backpressuredBevs;
    BEVToCallbackMap m_callbacks;
    boost::shared_ptr<voltdb::StatusListener> m_listener;
    bool m_invocationBlockedOnBackpressure;
    bool m_loopBreakRequested;
    bool m_isDraining;
    bool m_instanceIdIsSet;
    int32_t m_outstandingRequests;
    //Identifier of the database instance this client is connected to
    int64_t m_clusterStartTime;
    int32_t m_leaderAddress;

    std::string m_username;
    unsigned char m_passwordHash[20];
    const int32_t m_maxOutstandingRequests;

    bool m_ignoreBackpressure;
    bool m_useClientAffinity;
    //Flag to be set if topology is changed: node disconnected/rejoined
    bool m_updateHashinator;

    std::list<boost::shared_ptr<PendingConnection> > m_pendingConnectionList;
    boost::atomic<size_t> m_pendingConnectionSize;
    boost::mutex m_pendingConnectionLock;

    int m_wakeupPipe[2];
    boost::mutex m_wakeupPipeLock;

    ClientLogger* m_pLogger;
};
}
#endif /* VOLTDB_CLIENTIMPL_H_ */
