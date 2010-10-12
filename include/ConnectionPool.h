/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

#ifndef VOLT_CONNECTION_POOL_H_
#define VOLT_CONNECTION_POOL_
#include "Client.h"
#include "StatusListener.h"
#include <boost/unordered_map.hpp>
#include <boost/thread/tss.hpp>
#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <string>

namespace voltdb {

class ConnectionPool;
class ClientStuff;
typedef std::vector<boost::shared_ptr<ClientStuff> > ClientSet;
typedef boost::unordered_map<std::string, std::vector<boost::shared_ptr<ClientStuff> > > ClientMap;

void cleanupOnScriptEnd(ClientSet *clients);

/*
 * A VoltDB connection pool.
 */
class ConnectionPool {
    friend void cleanupOnScriptEnd(ClientSet *);
public:
    ConnectionPool();
    virtual ~ConnectionPool();

    /*
     * Retrieve a client that connects to the specified hostname and port using the provided username and password
     */
    voltdb::Client acquireClient(std::string hostname, std::string username, std::string password, short port = 21212) throw (voltdb::ConnectException, voltdb::LibEventException, voltdb::Exception);

    /*
     * Retrieve a client that connects to the specified hostname and port using the provided username and password
     */
    voltdb::Client acquireClient(std::string hostname, std::string username, std::string password, StatusListener *listener, short port = 21212) throw (voltdb::ConnectException, voltdb::LibEventException, voltdb::Exception);

    /*
     * Release any unreleased clients associated with this thread/script
     */
    void onScriptEnd();

    /*
     * Retrieve the global connection pool
     */
    static voltdb::ConnectionPool* pool();

private:
    boost::thread_specific_ptr<ClientSet> *m_borrowedClients;
    ClientMap m_clients;
    boost::mutex m_lock;
};

void onLoad();
void onUnload();
void onScriptEnd();

}
#endif /* VOLT_CONNECTION_POOL_H_ */
