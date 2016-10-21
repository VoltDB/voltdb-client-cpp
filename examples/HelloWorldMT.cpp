/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#define __STDC_CONSTANT_MACROS
#define __STDC_LIMIT_MACROS

#include <vector>
#include <boost/shared_ptr.hpp>
#include "Client.h"
#include "Table.h"
#include "TableIterator.h"
#include "Row.hpp"
#include "WireType.h"
#include "Parameter.hpp"
#include "ParameterSet.hpp"
#include "ProcedureCallback.hpp"
#include "ClientConfig.h"

static void *runInvocations(void *);

int main(int argc, char **argv) {
    /*
     * Instantiate a client and connect to the database.
     * SHA-256 can be used as of VoltDB5.2 by specifying voltdb::HASH_SHA256
     */
    voltdb::ClientConfig config("myusername", "mypassword", voltdb::HASH_SHA1);
    voltdb::Client client = voltdb::Client::create(config);
    client.createConnection("localhost", 21212, true);
    pthread_t th;
    pthread_create(&th, NULL, &runInvocations, &client);
    std::cout << "Thread is created: " << __FUNCTION__ << std::endl;
    std::cout << "Thread is running: " << __FUNCTION__ << std::endl;
    
    while (true) {
        try {
            client.run();
            std::cout << "Run Ended" << __FUNCTION__ << std::endl;
            std::cout.flush();            
        } catch (voltdb::Exception ex) {
            std::cout << ex.what() << __FUNCTION__ << std::endl ;
            std::cout.flush();
        }
    }
    pthread_join(th, NULL);
    
}

class CountingCallback : public voltdb::ProcedureCallback {
public:
    CountingCallback(int32_t count) : m_count(count) {}

    bool callback(voltdb::InvocationResponse response) throw (voltdb::Exception) {
        m_count--;

        //Print the error response if there was a problem
        if (response.failure()) {
            std::cout << response.toString();
        }

        //If the callback has been invoked count times, return true to break event loop
        if (m_count == 0) {
            return true;
        } else {
            return false;
        }
    }
private:
    int32_t m_count;
};

static void *runInvocations(void * cl) {
    voltdb::Client *client = (voltdb::Client *)(cl);
    assert(client);
    std::cout << "Thread has started " << __FUNCTION__ << std::endl;
    int i = 0;
    char str[1024];
    boost::shared_ptr<CountingCallback> callback(new CountingCallback(5));
    sleep(4);
    while (true) {
        try {            
            /*
             * Describe the stored procedure to be invoked
             */
            std::vector<voltdb::Parameter> parameterTypes(3);
            parameterTypes[0] = voltdb::Parameter(voltdb::WIRE_TYPE_STRING);
            parameterTypes[1] = voltdb::Parameter(voltdb::WIRE_TYPE_STRING);
            parameterTypes[2] = voltdb::Parameter(voltdb::WIRE_TYPE_STRING);
            voltdb::Procedure procedure("Insert", parameterTypes);

            voltdb::InvocationResponse response;
            /*
             * Load the database.
             */
            voltdb::ParameterSet* params = procedure.params();
            sprintf(str, "%s%d", "English", i++);
            params->addString(str).addString("Hello").addString("World");
            client->invoke(procedure, callback);

#if 0            
            if (response.failure()) { std::cout << response.toString() << std::endl; }
            params->addString("French").addString("Bonjour").addString("Monde");
            response = client->invoke(procedure);
            if (response.failure()) { std::cout << response.toString(); }

            params->addString("Spanish").addString("Hola").addString("Mundo");
            response = client->invoke(procedure);
            if (response.failure()) { std::cout << response.toString(); }

            params->addString("Danish").addString("Hej").addString("Verden");
            response = client->invoke(procedure);
            if (response.failure()) { std::cout << response.toString(); }

            params->addString("Italian").addString("Ciao").addString("Mondo");
            response = client->invoke(procedure);
            if (response.failure()) { std::cout << response.toString(); }

            /*
             * Describe procedure to retrieve message
             */
            parameterTypes.resize( 1, voltdb::Parameter(voltdb::WIRE_TYPE_STRING));
            voltdb::Procedure selectProc("Select", parameterTypes);

            /*
             * Retrieve the message
             */
            selectProc.params()->addString("Spanish");
            response = client->invoke(selectProc);
#endif
            /*
             * Print the response
             */
            std::cout << response.toString() << __FUNCTION__ << std::endl;

        } catch (voltdb::Exception ex) {
            std::cout << ex.what() <<  __FUNCTION__ << std::endl;
        }
    }
    
}
