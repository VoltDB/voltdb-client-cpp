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

#include <cppunit/TestCase.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestFixture.h>
#include "ConnectionPool.h"
#include "Client.h"

namespace voltdb {

/**
  * For now, connection pool testing should only be run when there is a local running Volt server
  * listening to port 21212
  */

class ConnectionPoolTest : public CppUnit::TestFixture {
CPPUNIT_TEST_SUITE( ConnectionPoolTest );
CPPUNIT_TEST( testReuse );
CPPUNIT_TEST( testReturn );
CPPUNIT_TEST( testClose );
CPPUNIT_TEST_SUITE_END();

public:
    void setUp() {
        m_connectionPool.reset(new ConnectionPool());
    }

    void tearDown() {
        m_connectionPool.reset(NULL);
    }

    void testReuse() {
        voltdb::Client client = m_connectionPool->acquireClient("localhost", "program", "password", NULL, 21212);
        int numClientsBorrowed = m_connectionPool->numClientsBorrowed();

        voltdb::Client client_reuse = m_connectionPool->acquireClient("localhost", "program", "password", NULL, 21212);
        CPPUNIT_ASSERT(m_connectionPool->numClientsBorrowed() == numClientsBorrowed);
    }

    void testReturn() {
         std::cout << "RETURN TEST" << std::endl;
         voltdb::Client client = m_connectionPool->acquireClient("localhost", "program", "password", NULL, 21212);
         m_connectionPool->returnClient(client);
         voltdb::Client client2 = m_connectionPool->acquireClient("localhost", "program", "password", NULL, 21212);
         CPPUNIT_ASSERT(client == client2);
    }

    void testClose() {
         std::cout << "CLOSE TEST" << std::endl;
         voltdb::Client client = m_connectionPool->acquireClient("localhost", "program", "password", NULL, 21212);
         m_connectionPool->closeClientConnection(client);
         voltdb::Client client2 = m_connectionPool->acquireClient("localhost", "program", "password", NULL, 21212);
         bool c = !(client == client2);
         //make sure client is new
         CPPUNIT_ASSERT(c);
         //No exception should be thrown.
         m_connectionPool->closeClientConnection(client);
         client2 = m_connectionPool->acquireClient("localhost", "program", "password", NULL, 21212);
         m_connectionPool->returnClient(client2);
         m_connectionPool->closeClientConnection(client2);
    }

private:
    boost::scoped_ptr<ConnectionPool> m_connectionPool;
};

CPPUNIT_TEST_SUITE_REGISTRATION( ConnectionPoolTest );
}
