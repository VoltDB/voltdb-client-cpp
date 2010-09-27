/*
 * Client.cpp
 *
 *  Created on: Jun 11, 2010
 *      Author: aweisberg
 */
#include "Client.h"
#include "ClientImpl.h"

namespace voltdb {

Client Client::create(ClientConfig config) throw(voltdb::Exception, voltdb::LibEventException) {
    Client client(new ClientImpl(config));
    return client;
}

Client::Client(ClientImpl *impl) : m_impl(impl) {}

Client::~Client() {
}

void
Client::createConnection(
        std::string hostname,
        short port)
throw (voltdb::Exception, voltdb::ConnectException, voltdb::LibEventException) {
    m_impl->createConnection(hostname, port);
}

/*
 * Synchronously invoke a stored procedure and return a the response.
 */
InvocationResponse
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
Client::invoke(
        Procedure &proc,
        ProcedureCallback *callback)
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
