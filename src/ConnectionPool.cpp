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

#include "ConnectionPool.h"
#include <cassert>
#include <exception>
#include <iostream>
#include <boost/scoped_ptr.hpp>
#include <cstdio>
#include "InvocationResponse.hpp"
#include "ClientConfig.h"

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
            boost::shared_ptr<voltdb::ProcedureCallback> callback,
            InvocationResponse response) {
        if (m_listener != NULL) {
            return m_listener->uncaughtException(exception, callback, response);
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
    bool connectionActive(std::string hostname, int32_t connectionsActive) {
        if (m_listener != NULL) {
            bool retval = m_listener->connectionActive(hostname, connectionsActive);
            return retval;
        } else {
            return false;
        }
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
            DelegatingStatusListener *listener) :
    m_identifier(identifier), m_listener(listener), m_client(client)
    {}
    std::string m_identifier;
    DelegatingStatusListener *m_listener;
    voltdb::Client m_client;
};

/*
 * Short hand for an exception safe lock acquisition
 */
class LockGuard {
public:
    LockGuard(pthread_mutex_t &mutex) {
        m_mutex = &mutex;
        pthread_mutex_lock(m_mutex);
    }
    ~LockGuard() {
        pthread_mutex_unlock(m_mutex);
    }
private:
    pthread_mutex_t *m_mutex;
};

/*
 * Cleanup function used by thread local ptr to a list of clients
 * that were borrowed from the pool. It unsets the listener and returns the client
 * to the pool.
 */
void cleanupOnScriptEnd(void *ptr) {
    if (gPool != NULL) {
        LockGuard guard(gPool->m_lock);
        ClientSet *clients = reinterpret_cast<ClientSet*>(ptr);
        if (clients != NULL) {
            boost::scoped_ptr<ClientSet> guard(clients);
            for(ClientSet::iterator i = clients->begin(); i != clients->end(); i++) {
                (*i)->m_listener->m_listener = NULL;
                gPool->m_clients[(*i)->m_identifier].push_back(*i);
            }
            pthread_setspecific(gPool->m_borrowedClients, NULL);
        }
    }
}

ConnectionPool::ConnectionPool() {
    pthread_mutex_init(&m_lock, NULL);
    pthread_key_create(&m_borrowedClients, &cleanupOnScriptEnd);
}

/*
 * Destructor specifically deletes the thread local pointer because the cleanup method
 * needs acess to the members of ConnectionPool.
 */
ConnectionPool::~ConnectionPool() {
    pthread_mutex_destroy(&m_lock);
    pthread_key_delete(m_borrowedClients);
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
        unsigned short port,
        ClientAuthHashScheme sha)
throw (voltdb::Exception, voltdb::ConnectException, voltdb::LibEventException) {
    LockGuard guard(m_lock);
    ClientSet *clients = reinterpret_cast<ClientSet*>(pthread_getspecific(m_borrowedClients));
    if (clients == NULL) {
        clients = new ClientSet();
        pthread_setspecific( m_borrowedClients, static_cast<const void *>(clients));
    }
    char portBytes[16];
    unsigned int portInt = port;
    snprintf(portBytes, 16, "%d", portInt);
    std::string identifier = hostname + "," + std::string(portBytes) + "," + username + "," + password;

    // if a thread calls acquireClient() multiple times with the same identifier, reuse the same client
    for (ClientSet::iterator i = clients->begin(); i != clients->end(); i++) {
        if ((*i)->m_identifier == identifier) {
            return (*i)->m_client;
        }
    }

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
            clients->push_back(clientStuff);
            return clientStuff->m_client;
        }
    }

    // no connection available, make a new one
    DelegatingStatusListener *delegatingListener = new DelegatingStatusListener();
    Client client = voltdb::Client::create(ClientConfig( username, password, delegatingListener, sha));
    client.createConnection(hostname, port);
    boost::shared_ptr<ClientStuff> stuff(new ClientStuff(client, identifier, delegatingListener));
    stuff->m_listener->m_listener = listener;
    clients->push_back(stuff);
    return client;
}

voltdb::Client
ConnectionPool::acquireClient(
        std::string hostname,
        std::string username,
        std::string password,
        unsigned short port,
        ClientAuthHashScheme sha)
throw (voltdb::Exception, voltdb::ConnectException, voltdb::LibEventException) {
    return acquireClient(hostname, username, password, NULL, port, sha);
}

void ConnectionPool::returnClient(Client client) throw (voltdb::Exception) {
    LockGuard guard(m_lock);
    ClientSet *clients = reinterpret_cast<ClientSet*>(pthread_getspecific(m_borrowedClients));
    if (clients == NULL) {
        throw MisplacedClientException();
    }

    for (ClientSet::iterator i = clients->begin(); i != clients->end(); i++) {
        if ((*i)->m_client == client) {
            (*i)->m_listener->m_listener = NULL;
            m_clients[(*i)->m_identifier].push_back(*i);
            clients->erase(i);
            return;
        }
    }

    throw MisplacedClientException();
}

void ConnectionPool::closeClientConnection(Client client) throw (voltdb::Exception) {
    LockGuard guard(m_lock);
    ClientSet *clients = reinterpret_cast<ClientSet*>(pthread_getspecific(m_borrowedClients));
    if (clients == NULL) {
        //No clients closing a stale object or not owned by this thread.
        return;
    }

    for (ClientSet::iterator i = clients->begin(); i != clients->end(); i++) {
        if ((*i)->m_client == client) {
            client.close();
            (*i)->m_listener->m_listener = NULL;
            clients->erase(i);
            return;
        }
    }
}

/*
 * Return the number of clients held by this thread
 */
int ConnectionPool::numClientsBorrowed() {
    ClientSet *clients = reinterpret_cast<ClientSet*>(pthread_getspecific(m_borrowedClients));
    if (clients != NULL) {
        return clients->size();
    }
    return 0;
}

/*
 * Release any unreleased clients associated with this thread/script
 */
void ConnectionPool::onScriptEnd() {
    cleanupOnScriptEnd(pthread_getspecific(m_borrowedClients));
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
