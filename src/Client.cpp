/*
 * Client.cpp
 *
 *  Created on: Jun 11, 2010
 *      Author: aweisberg
 */
#include "Client.h"
#include "ClientImpl.h"

namespace voltdb {

class DummyStatusListener : public StatusListener {
public:
    StatusListener *m_listener;
    DummyStatusListener(StatusListener *listener) : m_listener(listener) {}
    bool uncaughtException(std::exception exception, boost::shared_ptr<voltdb::ProcedureCallback> callback) {
        return m_listener->uncaughtException(exception, callback);
    }
    bool connectionLost(std::string hostname, int32_t connectionsLeft) {
        bool retval = m_listener->connectionLost(hostname, connectionsLeft);
        return retval;
    }
    bool backpressure(bool hasBackpressure) {
        return m_listener->backpressure(hasBackpressure);
    }
};

Client Client::create() throw(voltdb::Exception, voltdb::LibEventException) {
    Client client(new ClientImpl());
    return client;
}
Client Client::create(boost::shared_ptr<voltdb::StatusListener> listener) throw(voltdb::Exception, voltdb::LibEventException) {
    Client client(new ClientImpl(listener));
    return client;
}
Client Client::create(voltdb::StatusListener *listener) throw(voltdb::Exception, voltdb::LibEventException) {
    boost::shared_ptr<voltdb::StatusListener> wrapper(new DummyStatusListener(listener));
    return Client::create(wrapper);
}

Client::Client(ClientImpl *impl) : m_impl(impl) {}

Client::~Client() {
}

void
Client::createConnection(
        std::string hostname,
        std::string username,
        std::string password,
        short port)
throw (voltdb::Exception, voltdb::ConnectException, voltdb::LibEventException) {
    m_impl->createConnection(hostname, username, password, port);
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
