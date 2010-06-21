/*
 * Client.cpp
 *
 *  Created on: Jun 11, 2010
 *      Author: aweisberg
 */
#include "Client.h"
#include "ClientImpl.h"

namespace voltdb {

boost::shared_ptr<Client> Client::create() throw(voltdb::LibEventException) {
    return boost::shared_ptr<Client>(new Client(new ClientImpl()));
}
boost::shared_ptr<Client> Client::create(boost::shared_ptr<voltdb::StatusListener> listener) throw(voltdb::LibEventException) {
    return boost::shared_ptr<Client>(new Client(new ClientImpl(listener)));
}

Client::Client(ClientImpl *impl) : m_impl(impl) {}

Client::~Client() {
    delete m_impl;
}

void
Client::createConnection(
        std::string hostname,
        std::string username,
        std::string password)
throw (voltdb::Exception, voltdb::ConnectException, voltdb::LibEventException) {
    m_impl->createConnection(hostname, username, password);
}

/*
 * Synchronously invoke a stored procedure and return a the response.
 */
boost::shared_ptr<InvocationResponse>
Client::invoke(Procedure &proc)
throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::UninitializedParamsException, voltdb::LibEventException) {
    return m_impl->invoke(proc);
}

void
Client::invoke(
        Procedure &proc,
        boost::shared_ptr<ProcedureCallback> callback)
throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::UninitializedParamsException, voltdb::LibEventException) {
    m_impl->invoke(proc, callback);
}

void
Client::runOnce()
throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::LibEventException) {
    m_impl->runOnce();
}

void
Client::run()
throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::LibEventException) {
    m_impl->run();
}

bool
Client::drain()
throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::LibEventException) {
    return m_impl->drain();
}
}
