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

class StupidCallback : public voltdb::ProcedureCallback {
    bool callback(boost::shared_ptr<voltdb::InvocationResponse> response) {
        printf("Status code %d\n", response->statusCode());
        return true;
    }
};

int main(int argc, char **argv) {
    printf ("%d\n", argc);
    assert(argv != NULL);
    printf("foo\n");
    voltdb::ScopedByteBuffer buf(8);
    buf.putInt32(static_cast<int32_t>(233));
    buf.position(0);
    printf("%d\n", buf.getInt32());
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
    std::vector<voltdb::Parameter> parameters;
    //parameters.push_back(voltdb::Parameter(voltdb::WIRE_TYPE_INTEGER));
    //parameters.push_back(voltdb::Parameter(voltdb::WIRE_TYPE_STRING));
    voltdb::Procedure noexist("Results", parameters);
    //voltdb::ParameterSet *params = noexist.params();
    //params->addInt32(45);
    //params->addString("woot!");
    boost::shared_ptr<voltdb::InvocationResponse> response = client->invoke(&noexist);
    printf("Response code is %d\n", response->statusCode());
    printf("Response string is %s\n", response->statusString().c_str());
    printf("Result count: %d\n", static_cast<int32_t>(response->results().size()));
    voltdb::TableIterator iterator = response->results()[0]->iterator();
    while (iterator.hasNext()) {
        voltdb::Row row = iterator.next();
        printf("%s,\t%jd\n", row.getString(0).c_str(), (intmax_t)row.getInt64(1));
    }
}
