/*
 * ClientImpl.h
 *
 *  Created on: Jun 21, 2010
 *      Author: aweisberg
 */

#ifndef VOLTDB_CLIENTIMPL_H_
#define VOLTDB_CLIENTIMPL_H_
#include <event2/event.h>
#include <event2/bufferevent.h>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include "ProcedureCallback.hpp"
#include "StatusListener.h"
#include "Procedure.hpp"
#include "Client.h"
#include <boost/shared_ptr.hpp>

namespace voltdb {

class CxnContext;
class MockVoltDB;
class ClientImpl {
    friend class MockVoltDB;
public:
    friend class Client;

    /*
     * Map from client data to the appropriate callback for a specific connection
     */
    typedef boost::unordered_map< int64_t, boost::shared_ptr<ProcedureCallback> > CallbackMap;

    /*
     * Map from buffer event (connection) to the connection's callback map
     */
    typedef boost::unordered_map< struct bufferevent*, boost::shared_ptr<CallbackMap> > BEVToCallbackMap;

    class Client;

    /*
     * Create a connection to the VoltDB process running at the specified host authenticating
     * using the provided username and password.
     * @param hostname Hostname or IP address to connect to
     * @param username Username to provide for authentication
     * @param password Password to provide for authentication
     * @param port Port to connect to
     * @throws voltdb::ConnectException An error occurs connecting or authenticating
     * @throws voltdb::LibEventException libevent returns an error code
     */
    void createConnection(std::string hostname, std::string username, std::string password, short port) throw (voltdb::Exception, voltdb::ConnectException, voltdb::LibEventException);

    /*
     * Synchronously invoke a stored procedure and return a the response.
     */
    InvocationResponse invoke(Procedure &proc) throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::UninitializedParamsException, voltdb::LibEventException);
    void invoke(Procedure &proc, boost::shared_ptr<ProcedureCallback> callback) throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::UninitializedParamsException, voltdb::LibEventException);
    void invoke(Procedure &proc, ProcedureCallback *callback) throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::UninitializedParamsException, voltdb::LibEventException);
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

private:
    ClientImpl() throw(voltdb::Exception, voltdb::LibEventException);
    ClientImpl(boost::shared_ptr<voltdb::StatusListener> listener) throw(voltdb::Exception, voltdb::LibEventException);

    struct event_base *m_base;
    int64_t m_nextRequestId;
    size_t m_nextConnectionIndex;
    std::vector<struct bufferevent*> m_bevs;
    boost::unordered_map<struct bufferevent *, boost::shared_ptr<CxnContext> > m_contexts;
    boost::unordered_set<struct bufferevent *> m_backpressuredBevs;
    BEVToCallbackMap m_callbacks;
    boost::shared_ptr<voltdb::StatusListener> m_listener;
    bool m_invocationBlockedOnBackpressure;
    bool m_loopBreakRequested;
    bool m_isDraining;
    int32_t m_outstandingRequests;
};
}
#endif /* VOLTDB_CLIENTIMPL_H_ */
