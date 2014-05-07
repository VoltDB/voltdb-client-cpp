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

#ifndef __STDC_CONSTANT_MACROS
#define __STDC_CONSTANT_MACROS
#endif
#ifndef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS
#endif

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
     */
    voltdb::ClientConfig clientConfig("program", "password");
    voltdb::Client client = voltdb::Client::create();
    voltdb::errType err = voltdb::errOk;
    client.createConnection(err, "localhost");

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
    params->addString(err, "Hello").addString(err, "World").addString(err, "English");
    client.invoke(err, procedure, callback);

    params->addString(err, "Bonjour").addString(err, "Monde").addString(err, "French");
    client.invoke(err, procedure, callback);

    params->addString(err, "Hola").addString(err, "Mundo").addString(err, "Spanish");
    client.invoke(err, procedure, callback);

    params->addString(err, "Hej").addString(err, "Verden").addString(err, "Danish");
    client.invoke(err, procedure, callback);

    params->addString(err, "Ciao").addString(err, "Mondo").addString(err, "Italian");
    client.invoke(err, procedure, callback);

    /*
     * Run the client event loop to poll the network and invoke callbacks.
     * The event loop will break on an error or when a callback returns true
     */
    client.run(err);

    /*
     * Describe procedure to retrieve message
     */
    parameterTypes.resize( 1, voltdb::Parameter(voltdb::WIRE_TYPE_STRING));
    voltdb::Procedure selectProc("Select", parameterTypes);

    /*
     * Retrieve the message
     */
    selectProc.params()->addString(err, "Spanish");
    client.invoke(err, selectProc, boost::shared_ptr<PrintingCallback>(new PrintingCallback()));

    /*
     * Invoke event loop
     */
    client.run(err);

    return 0;
}

