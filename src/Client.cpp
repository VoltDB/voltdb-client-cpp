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

void Client::interrupt() {
    return m_impl->interrupt();
}

bool
Client::operator==(const Client &other) {
    return m_impl == other.m_impl;
}
}
