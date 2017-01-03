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

/*
 * A callback that counts the number of times that it is invoked and returns true
 * when the counter reaches zero to instruct the client library to break out of the event loop.
 */
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

/*
 * A callback that prints the response it receives and then requests the event loop
 * break
 */
class PrintingCallback : public voltdb::ProcedureCallback {
public:
    bool callback(voltdb::InvocationResponse response) throw (voltdb::Exception) {
        std::cout << response.toString();
        return true;
    }
};

int main(int argc, char **argv) {
    /*
     * Instantiate a client and connect to the database.
     * SHA-256 can be used as of VoltDB5.2 by specifying voltdb::HASH_SHA256
     */
    voltdb::ClientConfig config("myusername", "mypassword", voltdb::HASH_SHA1);
    voltdb::Client client = voltdb::Client::create(config);
    client.createConnection("localhost");

    /*
     * Describe the stored procedure to be invoked
     */
    std::vector<voltdb::Parameter> parameterTypes(3);
    parameterTypes[0] = voltdb::Parameter(voltdb::WIRE_TYPE_STRING);
    parameterTypes[1] = voltdb::Parameter(voltdb::WIRE_TYPE_STRING);
    parameterTypes[2] = voltdb::Parameter(voltdb::WIRE_TYPE_STRING);
    voltdb::Procedure procedure("Insert", parameterTypes);

    boost::shared_ptr<CountingCallback> callback(new CountingCallback(5));

    /*
     * Load the database.
     */
    voltdb::ParameterSet* params = procedure.params();
    params->addString("English").addString("Hello").addString("World");
    client.invoke(procedure, callback);

    params->addString("French").addString("Bonjour").addString("Monde");
    client.invoke(procedure, callback);

    params->addString("Spanish").addString("Hola").addString("Mundo");
    client.invoke(procedure, callback);

    params->addString("Danish").addString("Hej").addString("Verden");
    client.invoke(procedure, callback);

    params->addString("Italian").addString("Ciao").addString("Mondo");
    client.invoke(procedure, callback);

    /*
     * Run the client event loop to poll the network and invoke callbacks.
     * The event loop will break on an error or when a callback returns true
     */
    client.run();

    /*
     * Describe procedure to retrieve message
     */
    parameterTypes.resize( 1, voltdb::Parameter(voltdb::WIRE_TYPE_STRING));
    voltdb::Procedure selectProc("Select", parameterTypes);

    /*
     * Retrieve the message
     */
    selectProc.params()->addString("Spanish");
    client.invoke(selectProc, boost::shared_ptr<PrintingCallback>(new PrintingCallback()));

    /*
     * Invoke event loop
     */
    client.run();

    return 0;
}

