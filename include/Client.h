/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

#ifndef VOLTDB_CLIENT_H_
#define VOLTDB_CLIENT_H_

#include <string>
#include "ProcedureCallback.hpp"
#include "Procedure.hpp"
#include "StatusListener.h"
#include <boost/shared_ptr.hpp>
#include <event2/event.h>
#include <event2/bufferevent.h>
#include <vector>
#include <boost/unordered/unordered_map.hpp>
#include <boost/unordered/unordered_set.hpp>

namespace voltdb {

/*
 * Internal type, not needed by user
 */
typedef boost::unordered_map< int64_t, boost::shared_ptr<ProcedureCallback> > CallbackMap;
/*
 * Internal type, not needed by user
 */
typedef boost::unordered_map< struct bufferevent*, boost::shared_ptr<CallbackMap> > BEVToCallbackMap;

class Client;

/*
 * Internal type, not needed by user
 */
class CxnContext {
public:
    CxnContext(Client *client, struct event_base *base,
            boost::unordered_set<struct bufferevent *> *backpressuredBevs,
            BEVToCallbackMap *callbacks,
            bool *invocationBlockedOnBackpressure,
            boost::shared_ptr<voltdb::StatusListener> listener,
            std::vector<std::pair<struct bufferevent *, boost::shared_ptr<CxnContext> > > *bevs,
            std::string hostname)
        : m_client(client), m_base(base), m_lengthOrMessage(true), m_nextLength(4),
          m_backpressuredBevs(backpressuredBevs),
          m_callbacks(callbacks), m_invocationBlockedOnBackpressure(invocationBlockedOnBackpressure),
          m_listener(listener),
          m_bevs(bevs),
          m_hostname(hostname) {}
    Client *m_client;
    struct event_base *m_base;
    bool m_lengthOrMessage;
    int32_t m_nextLength;
    boost::unordered_set<struct bufferevent *> *m_backpressuredBevs;
    BEVToCallbackMap *m_callbacks;
    bool *m_invocationBlockedOnBackpressure;
    boost::shared_ptr<voltdb::StatusListener> m_listener;
    std::vector<std::pair<struct bufferevent *, boost::shared_ptr<CxnContext> > > *m_bevs;
    std::string m_hostname;
};

/*
 * A VoltDB client for invoking stored procedures on a VoltDB instance. The client and the
 * shared pointers it returns are not thread safe. If you need more parallelism you run multiple processes
 * and connect each process to a subset of the nodes in your VoltDB cluster.
 *
 * Because the client is single threaded it has no dedicated thread for doing network IO and invoking
 * callbacks. Applications must call run() and runOnce() periodically to ensure that requests are sent
 * and responses are processed.
 */
class Client {
public:
    /*
     * Create a connection to the VoltDB process running at the specified host authenticating
     * using the provided username and password.
     * @param hostname Hostname or IP address to connect to
     * @param username Username to provide for authentication
     * @param password Password to provide for authentication
     * @throws voltdb::ConnectException An error occurs connecting or authenticating
     * @throws voltdb::LibEventException libevent returns an error code
     */
    void createConnection(std::string hostname, std::string username, std::string password) throw (voltdb::Exception, voltdb::ConnectException, voltdb::LibEventException);

    /*
     * Synchronously invoke a stored procedure and return a the response.
     */
    boost::shared_ptr<InvocationResponse> invoke(Procedure &proc) throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::UninitializedParamsException, voltdb::LibEventException);
    void invoke(Procedure &proc, boost::shared_ptr<ProcedureCallback> callback) throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::UninitializedParamsException, voltdb::LibEventException);
    void runOnce() throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::LibEventException);
    void run() throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::LibEventException);
    static boost::shared_ptr<Client> create() throw(voltdb::LibEventException);
    static boost::shared_ptr<Client> create(boost::shared_ptr<voltdb::StatusListener> listener) throw(voltdb::LibEventException);
    ~Client();
private:
    Client() throw(voltdb::LibEventException);
    Client(boost::shared_ptr<voltdb::StatusListener> listener) throw(voltdb::LibEventException);
    struct event_base *m_base;
    int64_t m_nextRequestId;
    size_t m_nextConnectionIndex;
    std::vector<std::pair<struct bufferevent *, boost::shared_ptr<CxnContext> > > m_bevs;
    boost::unordered_set<struct bufferevent *> m_backpressuredBevs;
    BEVToCallbackMap m_callbacks;
    boost::shared_ptr<voltdb::StatusListener> m_listener;
    bool m_invocationBlockedOnBackpressure;
    bool m_loopBreakRequested;
};
}

#endif /* VOLTDB_CLIENT_H_ */
