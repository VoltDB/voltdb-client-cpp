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
#include "ClientConfig.h"

int main(int argc, char **argv) {
    /*
     * Instantiate a client and connect to the database.
     */
    voltdb::ClientConfig config("program", "password");
    voltdb::Client client = voltdb::Client::create();
    client.createConnection("localhost");

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
    params->addString("Hello").addString("World").addString("English");
    response = client.invoke(procedure);
    if (response.failure()) { std::cout << response.toString() << std::endl; return -1; }

    params->addString("Bonjour").addString("Monde").addString("French");
    response = client.invoke(procedure);
    if (response.failure()) { std::cout << response.toString(); return -1; }

    params->addString("Hola").addString("Mundo").addString("Spanish");
    response = client.invoke(procedure);
    if (response.failure()) { std::cout << response.toString(); return -1; }

    params->addString("Hej").addString("Verden").addString("Danish");
    response = client.invoke(procedure);
    if (response.failure()) { std::cout << response.toString(); return -1; }

    params->addString("Ciao").addString("Mondo").addString("Italian");
    response = client.invoke(procedure);
    if (response.failure()) { std::cout << response.toString(); return -1; }

    /*
     * Describe procedure to retrieve message
     */
    parameterTypes.resize( 1, voltdb::Parameter(voltdb::WIRE_TYPE_STRING));
    voltdb::Procedure selectProc("Select", parameterTypes);

    /*
     * Retrieve the message
     */
    selectProc.params()->addString("Spanish");
    response = client.invoke(selectProc);

    /*
     * Print the response
     */
    std::cout << response.toString();

    if (response.statusCode() != voltdb::STATUS_CODE_SUCCESS) {
        return -1;
    } else {
        return 0;
    }
}

