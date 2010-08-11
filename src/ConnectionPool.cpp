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

#include "ConnectionPool.h"
#include <cassert>
#include <exception>
#include <iostream>
#include <boost/scoped_ptr.hpp>
#include <cstdio>

namespace voltdb {

/*
 * Global connection pool instance that is intialized when the library is loaded and deleted on unload
 */
static ConnectionPool *gPool;

/*
 * A delegating listener that forwards to another listener that can be NULL. It is also possible to change
 * the listener being delegated to on the fly.
 */
class DelegatingStatusListener : public StatusListener {
public:
    StatusListener *m_listener;
    bool m_connectionLost;

    DelegatingStatusListener() : m_listener(NULL), m_connectionLost(false) {}

    bool uncaughtException(
            std::exception exception,
            boost::shared_ptr<voltdb::ProcedureCallback> callback) {
        if (m_listener != NULL) {
            return m_listener->uncaughtException(exception, callback);
        }
        return false;
    }
    bool connectionLost(std::string hostname, int32_t connectionsLeft) {
        m_connectionLost = true;
        if (m_listener != NULL) {
            bool retval = m_listener->connectionLost(hostname, connectionsLeft);
            return retval;
        }
        return false;
    }
    bool backpressure(bool hasBackpressure) {
        if (m_listener != NULL) {
            return m_listener->backpressure(hasBackpressure);
        }
        return false;
    }
};

/*
 * A record for information associated with a client such as the delegating listener
 * and identifier. This can be used to return the client to the ClientMap after a script exits.
 */
class ClientStuff {
public:
    ClientStuff(
            voltdb::Client client,
            std::string identifier,
            boost::shared_ptr<DelegatingStatusListener> listener) :
    m_identifier(identifier), m_listener(listener), m_client(client)
    {}
    std::string m_identifier;
    boost::shared_ptr<DelegatingStatusListener> m_listener;
    voltdb::Client m_client;
};

/*
 * Short hand for an exception safe lock acquisition
 */
typedef boost::lock_guard<boost::mutex> LockGuard;

/*
 * Cleanup function used by thread local ptr to a list of clients
 * that were borrowed from the pool. It unsets the listener and returns the client
 * to the pool.
 */
void cleanupOnScriptEnd(ClientSet *clients) {
    if (clients != NULL) {
        boost::scoped_ptr<ClientSet> guard(clients);
        for(ClientSet::iterator i = clients->begin(); i != clients->end(); i++) {
            (*i)->m_listener->m_listener = NULL;
            if (gPool != NULL) {
                gPool->m_clients[(*i)->m_identifier].push_back(*i);
            }
        }
    }
}

ConnectionPool::ConnectionPool() :
        m_borrowedClients(new boost::thread_specific_ptr<ClientSet>(&cleanupOnScriptEnd)) {

}

/*
 * Destructor specifically deletes the thread local pointer because the cleanup method
 * needs acess to the members of ConnectionPool.
 */
ConnectionPool::~ConnectionPool() {
    /*
     * Must delete here so that cleanupOnScriptEnd can do the right thing with access to gPool
     */
    delete m_borrowedClients;
}

/*
 * Retrieve a client that is connected and authenticated
 * to the specified hostname and port. Will reuse an existing connection if one is available.
 */
voltdb::Client
ConnectionPool::acquireClient(
        std::string hostname,
        std::string username,
        std::string password,
        StatusListener *listener,
        short port)
throw (voltdb::Exception, voltdb::ConnectException, voltdb::LibEventException) {
    LockGuard guard(m_lock);
    if (m_borrowedClients->get() == NULL) {
        m_borrowedClients->reset(new ClientSet());
    }
    char portBytes[16];
    int portInt = port;
    snprintf(portBytes, 16, "%d", portInt);
    std::string identifier = hostname + "," + std::string(portBytes) + "," + username + "," + password;
    std::vector<boost::shared_ptr<ClientStuff> > *clientStuffs = &m_clients[identifier];

    while (clientStuffs->size() > 0) {
        boost::shared_ptr<ClientStuff> clientStuff = clientStuffs->back();
        clientStuffs->pop_back();

        // run the event loop once to verify the connection is still available
        clientStuff->m_client.runOnce();

        if (clientStuff->m_listener->m_connectionLost) {
            // if this connection is lost, try the next
            continue;
        } else {
            // otherwise return this connection
            clientStuff->m_listener->m_listener = listener;
            (*m_borrowedClients)->push_back(clientStuff);
            return clientStuff->m_client;
        }
    }

    // no connection available, make a new one
    boost::shared_ptr<DelegatingStatusListener> delegatingListener(new DelegatingStatusListener());
    Client client = voltdb::Client::create(delegatingListener);
    client.createConnection(hostname, username, password, port);
    boost::shared_ptr<ClientStuff> stuff(new ClientStuff(client, identifier, delegatingListener));
    (*m_borrowedClients)->push_back(stuff);
    return client;
}

voltdb::Client
ConnectionPool::acquireClient(
        std::string hostname,
        std::string username,
        std::string password,
        short port)
throw (voltdb::Exception, voltdb::ConnectException, voltdb::LibEventException) {
    return acquireClient(hostname, username, password, NULL, port);
}

/*
 * Release any unreleased clients associated with this thread/script
 */
void ConnectionPool::onScriptEnd() {
    LockGuard guard(m_lock);
    m_borrowedClients->reset();
}

/*
 * Invoked by the external language/script environment when the library is loaded.
 * Creates the connection pool
 */
void onLoad() {
    assert(gPool == NULL);
    gPool = new ConnectionPool();
}

/*
 * Invoked by the external language/script environment when the library is unloaded.
 * Deletes the connection pool closing all connections. Does not drain connections.
 */
void onUnload() {
    assert(gPool != NULL);
    delete gPool;
    gPool = NULL;
}

/*
 * Invoked by the external language/script environment when a script ends. Causes
 * all connections acquired by this thread to be returned.
 */
void onScriptEnd() {
    assert(gPool != NULL);
    gPool->onScriptEnd();
}

voltdb::ConnectionPool* ConnectionPool::pool() {
    assert(gPool != NULL);
    return gPool;
}

}
