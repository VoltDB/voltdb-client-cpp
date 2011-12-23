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

#include <cstdio>
#include <vector>
#include "Client.h"
#include "Table.h"
#include "TableIterator.h"
#include "Row.hpp"
#include "WireType.h"
#include "Parameter.hpp"
#include "ParameterSet.hpp"

bool connected = false;
int connections = 0;

void conn_callback(voltdb::CoreClient* client, voltdb::connection_event event);
void proc_callback_timeout(voltdb::CoreClient *client, voltdb::InvocationResponse response, void* payload);
void proc_callback_countdown(voltdb::CoreClient *client, voltdb::InvocationResponse response, void* payload);

void conn_callback(voltdb::CoreClient* client, voltdb::connection_event event) {
    ++connections;
    if (event.type == voltdb::CONNECTED) {
        connected = true;
    }
    printf("conn_callback with enum value %d and info \"%s\"\n", event.type, event.info.c_str());
    client->interrupt();
}

void proc_callback_timeout(voltdb::CoreClient *client, voltdb::InvocationResponse response, void* payload) {
    int *outstanding = reinterpret_cast<int*>(payload);
    (*outstanding)--;
    std::cout << response.toString() << std::endl;
}

void proc_callback_countdown(voltdb::CoreClient *client, voltdb::InvocationResponse response, void* payload) {
    int *outstanding = reinterpret_cast<int*>(payload);
    std::cout << response.toString() << std::endl;
    (*outstanding)--;
    if (*outstanding == 0)
        client->interrupt(); // return when no more work to do    
}

int main(int argc, char **argv) {
    int outstanding = 0;
    
    /*
     * Instantiate a client and connect to the database.
     */
    voltdb::CoreClient client(conn_callback);
    client.createConnection("localhost");
    
    // spin until connected
    int retcode = client.run(); // run one second at a time
    assert(retcode == voltdb::CoreClient::INTERRUPTED_OR_EARLY_EXIT);
    
    if (!connected) {
        exit(-1);
    }
    
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
    outstanding++;
    client.invoke(procedure, proc_callback_timeout, &outstanding);
    
    // call the run loop until this returns
    while (outstanding) {
        retcode = client.runWithTimeout(1000);
        assert(retcode == voltdb::CoreClient::TIMEOUT_ELAPSED);
    }
    
    params->addString("Bonjour").addString("Monde").addString("French");
    outstanding++;
    client.invoke(procedure, proc_callback_countdown, &outstanding);
    
    params->addString("Hola").addString("Mundo").addString("Spanish");
    outstanding++;
    client.invoke(procedure, proc_callback_countdown, &outstanding);
    
    params->addString("Hej").addString("Verden").addString("Danish");
    outstanding++;
    client.invoke(procedure, proc_callback_countdown, &outstanding);
    
    params->addString("Ciao").addString("Mondo").addString("Italian");
    outstanding++;
    client.invoke(procedure, proc_callback_countdown, &outstanding);
    
    // call the run loop until this returns
    retcode = client.run();
    assert(retcode == voltdb::CoreClient::INTERRUPTED_OR_EARLY_EXIT);
    
    /*
     * Describe procedure to retrieve message
     */
    parameterTypes.resize( 1, voltdb::Parameter(voltdb::WIRE_TYPE_STRING));
    voltdb::Procedure selectProc("Select", parameterTypes);
    
    /*
     * Retrieve the message
     */
    selectProc.params()->addString("Spanish");
    client.invoke(selectProc, proc_callback_timeout, &outstanding);
    
    // spin until it returns
    while (outstanding) {
        client.runOnce();
    }
}

