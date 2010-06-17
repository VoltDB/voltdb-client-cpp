/*
 * ByteBufferTest.cpp
 *
 *  Created on: Jun 10, 2010
 *      Author: aweisberg
 */
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <boost/shared_ptr.hpp>
#include "ByteBuffer.hpp"
#include "Client.h"
#include "Procedure.hpp"
#include "ProcedureCallback.hpp"
#include "InvocationResponse.hpp"
#include "ParameterSet.hpp"
#include "WireType.h"
#include "TableIterator.h"
#include "Row.hpp"
#include "Decimal.hpp"

class StupidCallback : public voltdb::ProcedureCallback {
public:
    StupidCallback(bool *callbackInvoked) : callbackInvoked(callbackInvoked) {}
    bool callback(boost::shared_ptr<voltdb::InvocationResponse> response) {
        std::cout << response->toString() << std::endl;
        *callbackInvoked = true;
        return false;
    }
    bool *callbackInvoked;
};

int main(int argc, char **argv) {
    printf ("%d\n", argc);
    assert(argv != NULL);
    printf("foo\n");
    voltdb::ScopedByteBuffer buf(8);
    buf.putInt64(static_cast<int64_t>(2398009703));
    buf.position(0);
    printf("%jd\n", (intmax_t)buf.getInt64());
    buf.position(0);
    buf.putDouble(3.1459);
    buf.position(0);
    printf("%f\n", buf.getDouble());
    buf.ensureCapacity(16);
    buf.putDouble(3.14589235);
    buf.position(8);
    printf("%f\n", buf.getDouble());
    //delete []storage;
    buf.position(0);
    buf.putString(0, std::string("123456789101"));
    bool wasNull;
    std::cout << buf.getString(0, wasNull) << std::endl;
    //boost::scoped_ptr<voltdb::ByteBuffer> bufDup(buf.duplicate());
    buf.getString(wasNull);
    //std::cout << bufDup->getString() << std::endl;
    boost::shared_ptr<voltdb::Client> client = voltdb::Client::create();
    if (client->createConnection("localhost", "", "")) {
        printf("Connect and authenticate succeded\n");
    } else {
        printf("Connected and athenticate failed\n");
    }
    bool callbackInvoked = false;
    boost::shared_ptr<StupidCallback> callback(new StupidCallback(&callbackInvoked));
    std::vector<voltdb::Parameter> parameters;
    parameters.push_back(voltdb::Parameter(voltdb::WIRE_TYPE_DECIMAL));
    //parameters.push_back(voltdb::Parameter(voltdb::WIRE_TYPE_STRING));
    voltdb::Procedure noexist("Results", parameters);
    voltdb::ParameterSet *params = noexist.params();
    params->addDecimal(voltdb::Decimal(std::string("12312431542.123124")));
    //params->addString("woot!");
    client->invoke(&noexist, callback);
    while (!callbackInvoked) {
        client->runOnce();
    }
}
