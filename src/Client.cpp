/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

Client Client::create(ClientConfig config) {
    Client client(new ClientImpl(config));
    return client;
}

Client::Client(ClientImpl *impl) : m_impl(impl) {}

Client::~Client() {
}

void
Client::createConnection(
        errType& err,
        std::string hostname,
        short port)
{
    m_impl->createConnection(err, hostname, port);
}

/*
 * Synchronously invoke a stored procedure and return a the response.
 */
InvocationResponse
Client::invoke(errType& err, Procedure &proc)
{
    return m_impl->invoke(err, proc);
}

void
Client::invoke(
        errType& err,
        Procedure &proc,
        boost::shared_ptr<ProcedureCallback> callback)
{
    m_impl->invoke(err, proc, callback);
}

void
Client::invoke(
        errType& err,
        Procedure &proc,
        ProcedureCallback *callback)
{
    m_impl->invoke(err, proc, callback);
}

void
Client::runOnce()
{
    m_impl->runOnce();
}

void
Client::run()
{
    m_impl->run();
}

bool
Client::drain()
{
    return m_impl->drain();
}

void Client::interrupt() {
    return m_impl->interrupt();
}
}
