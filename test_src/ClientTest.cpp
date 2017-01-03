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
#include <vector>
#include "Client.h"
#include "MockVoltDB.h"
#include "StatusListener.h"
#include "Parameter.hpp"
#include "ParameterSet.hpp"
#include "Procedure.hpp"
#include "WireType.h"
#include "ProcedureCallback.hpp"
#include "InvocationResponse.hpp"
#include "ClientConfig.h"

namespace voltdb {

class DelegatingListener : public StatusListener {
public:
   StatusListener *m_listener;
   DelegatingListener() : m_listener(NULL) {
   }
   virtual bool uncaughtException(
           std::exception exception,
           boost::shared_ptr<voltdb::ProcedureCallback> callback,
           InvocationResponse response) {
       if (m_listener != NULL) {
           return m_listener->uncaughtException(exception, callback, response);
       }
       return false;
   }
   virtual bool connectionLost(std::string hostname, int32_t connectionsLeft) {
       if (m_listener != NULL) {
           return m_listener->connectionLost(hostname, connectionsLeft);
       }
       return false;
   }
   virtual bool connectionActive(std::string hostname, int32_t connectionsActive) {
       if (m_listener != NULL) {
           return m_listener->connectionActive(hostname, connectionsActive);
       }
       return false;
   }
   virtual bool backpressure(bool hasBackpressure) {
       if (m_listener != NULL) {
           return m_listener->backpressure(hasBackpressure);
       }
       return false;
   }
};

class ClientTest : public CppUnit::TestFixture {
CPPUNIT_TEST_SUITE( ClientTest );
CPPUNIT_TEST( testConnect );
CPPUNIT_TEST( testSyncInvoke );
CPPUNIT_TEST( testSyncInvokeClose );
CPPUNIT_TEST( testLargeReply );
CPPUNIT_TEST_EXCEPTION( testInvokeSyncNoConnections, voltdb::NoConnectionsException );
CPPUNIT_TEST_EXCEPTION( testInvokeAsyncNoConnections, voltdb::NoConnectionsException );
CPPUNIT_TEST_EXCEPTION( testRunNoConnections, voltdb::NoConnectionsException );
CPPUNIT_TEST_EXCEPTION( testRunOnceNoConnections, voltdb::NoConnectionsException );
CPPUNIT_TEST_EXCEPTION( testNullCallback, voltdb::NullPointerException );
CPPUNIT_TEST( testLostConnectionBreaksEventLoop );
CPPUNIT_TEST( testBreakEventLoopViaCallback );
CPPUNIT_TEST( testCallbackThrows );
CPPUNIT_TEST( testBackpressure );
CPPUNIT_TEST( testDrain );
CPPUNIT_TEST( testLostConnectionDuringDrain );
CPPUNIT_TEST( testSynchronousInvocations );
CPPUNIT_TEST_EXCEPTION( testLostConnection, voltdb::NoConnectionsException );
CPPUNIT_TEST_SUITE_END();

public:
    void setUp() {
        m_dlistener = new boost::shared_ptr<DelegatingListener>(new DelegatingListener());
        ClientConfig config = ClientConfig("hello", "world", *m_dlistener);
        m_voltdb.reset(new MockVoltDB(Client::create(config)));
        m_client = m_voltdb->client();
        m_client->setClientAffinity(false);
    }

    void tearDown() {
        delete m_dlistener;
        m_voltdb.reset(NULL);
    }

    void testConnect() {
        m_client->createConnection("localhost");
    }

    void testSyncInvoke() {
        m_client->createConnection("localhost");
        std::vector<Parameter> signature;
        signature.push_back(Parameter(WIRE_TYPE_STRING));
        signature.push_back(Parameter(WIRE_TYPE_STRING));
        signature.push_back(Parameter(WIRE_TYPE_STRING));
        Procedure proc("Insert", signature);
        ParameterSet *params = proc.params();
        params->addString("Hello").addString("World").addString("English");
        m_voltdb->filenameForNextResponse("invocation_response_success.msg");
        InvocationResponse response = (m_client)->invoke(proc);
        CPPUNIT_ASSERT(response.success());
        CPPUNIT_ASSERT(response.statusString() == "");
        CPPUNIT_ASSERT(response.appStatusCode() == -128);
        CPPUNIT_ASSERT(response.appStatusString() == "");
        CPPUNIT_ASSERT(response.results().size() == 1);
    }

    void testSyncInvokeClose() {
        m_client->createConnection("localhost");
        std::vector<Parameter> signature;
        signature.push_back(Parameter(WIRE_TYPE_STRING));
        signature.push_back(Parameter(WIRE_TYPE_STRING));
        signature.push_back(Parameter(WIRE_TYPE_STRING));
        Procedure proc("Insert", signature);
        ParameterSet *params = proc.params();
        params->addString("Hello").addString("World").addString("English");
        m_voltdb->filenameForNextResponse("invocation_response_success.msg");
        InvocationResponse response = (m_client)->invoke(proc);
        m_client->close();
        CPPUNIT_ASSERT(response.success());
        CPPUNIT_ASSERT(response.statusString() == "");
        CPPUNIT_ASSERT(response.appStatusCode() == -128);
        CPPUNIT_ASSERT(response.appStatusString() == "");
        CPPUNIT_ASSERT(response.results().size() == 1);
        bool thrown = false;
        try {
            response = (m_client)->invoke(proc);
        } catch (const voltdb::NoConnectionsException &e){
            thrown = true;
        }
        CPPUNIT_ASSERT(thrown);
        //make sure we can reconnect and invoke
        m_client->createConnection("localhost");
        ParameterSet *params2 = proc.params();
        params2->addString("Hello").addString("World").addString("English");
        response = (m_client)->invoke(proc);
        CPPUNIT_ASSERT(response.success());
        CPPUNIT_ASSERT(response.statusString() == "");
        CPPUNIT_ASSERT(response.appStatusCode() == -128);
        CPPUNIT_ASSERT(response.appStatusString() == "");
        CPPUNIT_ASSERT(response.results().size() == 1);
    }

    class SyncCallback : public ProcedureCallback {
    public:
        InvocationResponse m_response;
        bool m_hasResponse;
        SyncCallback() : m_hasResponse(false) {}
        virtual bool callback(InvocationResponse response) throw (voltdb::Exception) {
            m_response = response;
            m_hasResponse = true;
            return false;
        }
    };

    void testAsyncInvoke() {
        (m_client)->createConnection("localhost");
        std::vector<Parameter> signature;
        signature.push_back(Parameter(WIRE_TYPE_STRING));
        signature.push_back(Parameter(WIRE_TYPE_STRING));
        signature.push_back(Parameter(WIRE_TYPE_STRING));
        Procedure proc("Insert", signature);
        ParameterSet *params = proc.params();
        params->addString("Hello").addString("World").addString("English");
        m_voltdb->filenameForNextResponse("invocation_response_success.msg");

        SyncCallback *cb = new SyncCallback();
        boost::shared_ptr<ProcedureCallback> callback(cb);
        (m_client)->invoke(proc, callback);
        while (!cb->m_hasResponse) {
            (m_client)->runOnce();
        }
        InvocationResponse response = cb->m_response;
        CPPUNIT_ASSERT(response.success());
        CPPUNIT_ASSERT(response.statusString() == "");
        CPPUNIT_ASSERT(response.appStatusCode() == -128);
        CPPUNIT_ASSERT(response.appStatusString() == "");
        CPPUNIT_ASSERT(response.results().size() == 0);
    }

    // ENG-2553. Connection shouldn't timeout.
    void testLargeReply() {
        (m_client)->createConnection("localhost");
        std::vector<Parameter> signature;
        signature.push_back(Parameter(WIRE_TYPE_STRING));
        signature.push_back(Parameter(WIRE_TYPE_STRING));
        signature.push_back(Parameter(WIRE_TYPE_STRING));
        Procedure proc("Insert", signature);
        ParameterSet *params = proc.params();
        params->addString("Hello").addString("World").addString("English");
        m_voltdb->filenameForNextResponse("mimicLargeReply");
        InvocationResponse response = (m_client)->invoke(proc);
        CPPUNIT_ASSERT(response.success());
    }

    void testInvokeSyncNoConnections() {
        std::vector<Parameter> signature;
        Procedure proc("foo", signature);
        proc.params();
        (m_client)->invoke(proc);
    }

    void testInvokeAsyncNoConnections() {
        std::vector<Parameter> signature;
        Procedure proc("foo", signature);
        proc.params();
        SyncCallback *cb = new SyncCallback();
        boost::shared_ptr<ProcedureCallback> callback(cb);
        (m_client)->invoke(proc, callback);
    }

    void testRunNoConnections() {
        (m_client)->run();
    }

    void testRunOnceNoConnections() {
        (m_client)->runOnce();
    }

    void testNullCallback() {
        std::vector<Parameter> signature;
        Procedure proc("foo", signature);
        proc.params();
        (m_client)->invoke(proc, boost::shared_ptr<ProcedureCallback>());
    }

    void testLostConnection() {
        class Listener : public StatusListener {
        public:
            bool lostReported;
            bool activeReported;
           Listener() : lostReported(false), activeReported(false) {}
            virtual bool uncaughtException(
                    std::exception exception,
                    boost::shared_ptr<voltdb::ProcedureCallback> callback,
                    InvocationResponse response) {
                CPPUNIT_ASSERT(false);
                return false;
            }
            virtual bool connectionLost(std::string hostname, int32_t connectionsLeft) {
                lostReported = true;
                CPPUNIT_ASSERT(connectionsLeft == 0);
                return false;
            }
            virtual bool connectionActive(std::string hostname, int32_t connectionsLeft) {
                activeReported = true;
                return true;
            }
            virtual bool backpressure(bool hasBackpressure) {
                CPPUNIT_ASSERT(false);
                return false;
            }
        }  listener;
        (*m_dlistener)->m_listener = &listener;
        (m_client)->createConnection("localhost");

        std::vector<Parameter> signature;
        Procedure proc("Insert", signature);
        proc.params();

        SyncCallback *cb = new SyncCallback();
        boost::shared_ptr<ProcedureCallback> callback(cb);
        (m_client)->invoke(proc, callback);
        m_voltdb->hangupOnRequestCount(1);

        proc.params();
        InvocationResponse syncResponse = (m_client)->invoke(proc);
        CPPUNIT_ASSERT(syncResponse.failure());
        CPPUNIT_ASSERT(syncResponse.statusCode() == voltdb::STATUS_CODE_CONNECTION_LOST);
        CPPUNIT_ASSERT(syncResponse.results().size() == 0);

        InvocationResponse response = cb->m_response;
        CPPUNIT_ASSERT(response.failure());
        CPPUNIT_ASSERT(response.statusCode() == voltdb::STATUS_CODE_CONNECTION_LOST);
        CPPUNIT_ASSERT(response.results().size() == 0);
        CPPUNIT_ASSERT(listener.lostReported);
        //Invoke should throw exception.
        syncResponse = (m_client)->invoke(proc);
    }

    void testPendingConnection() {
        m_voltdb.reset(0);
        Client client = Client::create(ClientConfig("hello", "world", *m_dlistener));
        m_client = &client;
        m_client->createConnection("localhost", 21212, true);
        std::vector<Parameter> signature;
        signature.push_back(Parameter(WIRE_TYPE_STRING));
        signature.push_back(Parameter(WIRE_TYPE_STRING));
        signature.push_back(Parameter(WIRE_TYPE_STRING));
        Procedure proc("Insert", signature);
        ParameterSet *params = proc.params();
        params->addString("Hello").addString("World").addString("English");
        InvocationResponse response = (m_client)->invoke(proc);
        CPPUNIT_ASSERT(response.failure());
        m_voltdb.reset(new MockVoltDB(*m_client));
        m_voltdb->filenameForNextResponse("invocation_response_success.msg");
        response = (m_client)->invoke(proc);
        CPPUNIT_ASSERT(response.success());
        CPPUNIT_ASSERT(response.statusString() == "");
        CPPUNIT_ASSERT(response.appStatusCode() == -128);
        CPPUNIT_ASSERT(response.appStatusString() == "");
        CPPUNIT_ASSERT(response.results().size() == 0);
    }

    void testLostConnectionBreaksEventLoop() {
        class Listener : public StatusListener {
        public:
            virtual bool uncaughtException(
                    std::exception exception,
                    boost::shared_ptr<voltdb::ProcedureCallback> callback,
                    InvocationResponse response) {
                CPPUNIT_ASSERT(false);
                return false;
            }
            virtual bool connectionLost(std::string hostname, int32_t connectionsLeft) {
                CPPUNIT_ASSERT(connectionsLeft == 1);
                return true;
            }
            virtual bool connectionActive(std::string hostname, int32_t connectionsLeft) {
                return true;
            }
            virtual bool backpressure(bool hasBackpressure) {
                CPPUNIT_ASSERT(false);
                return false;
            }
        }  listener;
        (*m_dlistener)->m_listener = &listener;
        (m_client)->createConnection("localhost");
        (m_client)->createConnection("localhost");

        std::vector<Parameter> signature;
        Procedure proc("Insert", signature);
        proc.params();

        SyncCallback *cb = new SyncCallback();
        boost::shared_ptr<ProcedureCallback> callback(cb);
        (m_client)->invoke(proc, callback);
        m_voltdb->hangupOnRequestCount(1);

        (m_client)->run();
        (m_client)->runOnce();
    }

    class BreakingSyncCallback : public ProcedureCallback {
    public:
        InvocationResponse m_response;
        virtual bool callback(InvocationResponse response) throw (voltdb::Exception) {
            m_response = response;
            return true;
        }
    };

    void testBreakEventLoopViaCallback() {
        (m_client)->createConnection("localhost");
        std::vector<Parameter> signature;
        signature.push_back(Parameter(WIRE_TYPE_STRING));
        signature.push_back(Parameter(WIRE_TYPE_STRING));
        signature.push_back(Parameter(WIRE_TYPE_STRING));
        Procedure proc("Insert", signature);
        ParameterSet *params = proc.params();
        params->addString("Hello").addString("World").addString("English");
        m_voltdb->filenameForNextResponse("invocation_response_success.msg");

        BreakingSyncCallback *cb = new BreakingSyncCallback();
        boost::shared_ptr<ProcedureCallback> callback(cb);
        (m_client)->invoke(proc, callback);

        (m_client)->run();
    }

    class ThrowingCallback : public ProcedureCallback {
    public:
        virtual bool callback(InvocationResponse response) throw (voltdb::Exception) {
            throw voltdb::Exception();
        }
    };

    void testCallbackThrows() {
        class Listener : public StatusListener {
        public:
            bool lostReported;
            bool activeReported;
           Listener() : lostReported(false), activeReported(false) {}
            virtual bool uncaughtException(
                    std::exception exception,
                    boost::shared_ptr<voltdb::ProcedureCallback> callback,
                    InvocationResponse response) {
                lostReported = true;
                return true;
            }
            virtual bool connectionLost(std::string hostname, int32_t connectionsLeft) {
                lostReported = true;
                CPPUNIT_ASSERT(connectionsLeft == 0);
                return false;
            }
            virtual bool connectionActive(std::string hostname, int32_t connectionsLeft) {
                activeReported = true;
                return true;
            }
            virtual bool backpressure(bool hasBackpressure) {
                CPPUNIT_ASSERT(false);
                return false;
            }
        }  listener;

        (*m_dlistener)->m_listener = &listener;

        (m_client)->createConnection("localhost");
        std::vector<Parameter> signature;
        signature.push_back(Parameter(WIRE_TYPE_STRING));
        signature.push_back(Parameter(WIRE_TYPE_STRING));
        signature.push_back(Parameter(WIRE_TYPE_STRING));
        Procedure proc("Insert", signature);
        ParameterSet *params = proc.params();
        params->addString("Hello").addString("World").addString("English");
        m_voltdb->filenameForNextResponse("invocation_response_success.msg");

        ThrowingCallback *cb = new ThrowingCallback();
        boost::shared_ptr<ProcedureCallback> callback(cb);
        (m_client)->invoke(proc, callback);

        (m_client)->run();
    }

    void testBackpressure() {
        class Listener : public StatusListener {
        public:
            bool reported;
           Listener() : reported(false) {}
            virtual bool uncaughtException(
                    std::exception exception,
                    boost::shared_ptr<voltdb::ProcedureCallback> callback,
                    InvocationResponse response) {
                CPPUNIT_ASSERT(false);
                return true;
            }
            virtual bool connectionLost(std::string hostname, int32_t connectionsLeft) {
                CPPUNIT_ASSERT(false);
                return false;
            }
            virtual bool connectionActive(std::string hostname, int32_t connectionsLeft) {
                return true;
            }
            virtual bool backpressure(bool hasBackpressure) {
                CPPUNIT_ASSERT(hasBackpressure);
                reported = true;
                return true;
            }
        }  listener;
        (*m_dlistener)->m_listener = &listener;

        (m_client)->createConnection("localhost");
        std::vector<Parameter> signature;
        Procedure proc("Insert", signature);
        SyncCallback *cb = new SyncCallback();
        boost::shared_ptr<ProcedureCallback> callback(cb);
        m_voltdb->dontRead();
        while (!listener.reported) {
            (m_client)->invoke(proc, callback);
            (m_client)->runOnce();
        }
    }

    class CountingCallback : public voltdb::ProcedureCallback {
    public:
        CountingCallback(int32_t count) : m_count(count) {}

        bool callback(voltdb::InvocationResponse response) throw (voltdb::Exception) {
            m_count--;

            CPPUNIT_ASSERT(response.success());
            return false;
        }
        int32_t m_count;
    };

    void testDrain() {
        m_voltdb->filenameForNextResponse("invocation_response_success.msg");
        (m_client)->createConnection("localhost");
        std::vector<Parameter> signature;
        Procedure proc("Insert", signature);

        CountingCallback *cb = new CountingCallback(5);
        boost::shared_ptr<ProcedureCallback> callback(cb);
        for (int ii = 0; ii < 5; ii++) {
            (m_client)->invoke( proc, callback);
        }
        (m_client)->drain();
        CPPUNIT_ASSERT(cb->m_count == 0);
        CPPUNIT_ASSERT(! m_client->isDraining());
    }

    class CountingSuccessAndConnectionLost : public voltdb::ProcedureCallback {
    public:
        CountingSuccessAndConnectionLost() : m_success(0), m_connectionLost(0) {}

        bool callback(voltdb::InvocationResponse response) throw (voltdb::Exception) {
            if (response.success()) {
                m_success++;
            } else {
                CPPUNIT_ASSERT(response.statusCode() == voltdb::STATUS_CODE_CONNECTION_LOST);
                m_connectionLost++;
            }
            return false;
        }
        int32_t m_success;
        int32_t m_connectionLost;
    };

    void testLostConnectionDuringDrain() {
        m_voltdb->filenameForNextResponse("invocation_response_success.msg");
        (m_client)->createConnection("localhost");
        std::vector<Parameter> signature;
        Procedure proc("Insert", signature);

        CountingSuccessAndConnectionLost *cb = new CountingSuccessAndConnectionLost();
        boost::shared_ptr<ProcedureCallback> callback(cb);
        for (int ii = 0; ii < 5; ii++) {
            (m_client)->invoke( proc, callback);
        }
        m_voltdb->hangupOnRequestCount(3);
        (m_client)->drain();
        CPPUNIT_ASSERT(cb->m_success == 2);
        CPPUNIT_ASSERT(cb->m_connectionLost == 3);
    }

    void testSynchronousInvocations() {
        // Regression test for ENG-10480

        m_voltdb->filenameForNextResponse("invocation_response_success.msg");
        m_client->createConnection("localhost");

        std::vector<Parameter> signature;
        Procedure proc("Insert", signature);

        CountingSuccessAndConnectionLost *cb = new CountingSuccessAndConnectionLost();
        boost::shared_ptr<ProcedureCallback> callback(cb);

        // Queue up two async SP invocations
        for (int ii = 0; ii < 2; ii++) {
            m_client->invoke(proc, callback);
        }

        // Now do a synchronized invocation
        InvocationResponse response = m_client->invoke(proc);
        CPPUNIT_ASSERT(response.statusCode() == STATUS_CODE_SUCCESS);
        CPPUNIT_ASSERT(! m_client->isDraining());

        // Queued invocations will have been processed before the synchronous invocation.
        CPPUNIT_ASSERT(cb->m_success == 2);
        CPPUNIT_ASSERT(cb->m_connectionLost == 0);
    }

private:
    Client *m_client;
    boost::scoped_ptr<MockVoltDB> m_voltdb;
    boost::shared_ptr<DelegatingListener> *m_dlistener;
};
CPPUNIT_TEST_SUITE_REGISTRATION( ClientTest );
}
