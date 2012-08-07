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

#include <cppunit/TestCase.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestFixture.h>
#include <boost/scoped_ptr.hpp>
#include <iostream>
#include <exception>
#include <cstdio>
#include "Exception.hpp"
#include "ByteBuffer.hpp"
#include "AuthenticationRequest.hpp"
#include "AuthenticationResponse.hpp"
#include "Parameter.hpp"
#include "ParameterSet.hpp"
#include "Procedure.hpp"
#include "WireType.h"
#include "Decimal.hpp"
#include "InvocationResponse.hpp"
#include "Table.h"
#include "TableIterator.h"
#include "Row.hpp"
#include "sha1.h"

namespace voltdb {
class SerializationTest : public CppUnit::TestFixture {
CPPUNIT_TEST_SUITE( SerializationTest );
CPPUNIT_TEST(testAuthenticationRequest);
CPPUNIT_TEST(testAuthenticationResponse);
CPPUNIT_TEST(testInvocationAllParams);
CPPUNIT_TEST(testInvocationResponseSuccess);
CPPUNIT_TEST(testInvocationResponseFailCV);
CPPUNIT_TEST(testInvocationResponseSelect);
CPPUNIT_TEST(testSerializedTable);
CPPUNIT_TEST_SUITE_END();
public:
SharedByteBuffer fileAsByteBuffer(std::string filename) {

    int ret = 0;

    std::string path = "test_src/" + filename;
    FILE *fp = fopen(path.c_str(), "r");
    assert(fp);
    ret = fseek(fp, 0L, SEEK_END);
    assert(!ret);
    size_t size = ftell(fp);
    ret = fseek(fp, 0L, SEEK_SET);
    assert(!ret);
    assert(ftell(fp) == 0);

    char *buffer = new char[size];
    SharedByteBuffer b(buffer, (int)size);

    size_t bytes_read = fread(buffer, 1, size, fp);
    if (bytes_read != size) {
        printf("Failed to read file at %s with size %d (read: %d)\n",
            path.c_str(), (int)size, (int)bytes_read);
    }
    assert(bytes_read == size);

    fclose(fp);

    return b;
}

void testAuthenticationRequest() {
    SharedByteBuffer original = fileAsByteBuffer("authentication_request.msg");
    SharedByteBuffer generated(new char[8192], 8192);
    unsigned char hashedPassword[20];
    std::string password("world");
    SHA1_CTX context;
    SHA1_Init(&context);
    SHA1_Update( &context, reinterpret_cast<const unsigned char*>(password.data()), password.size());
    SHA1_Final ( &context, hashedPassword);
    AuthenticationRequest request( "hello", "database", hashedPassword);
    errType err = errOk;
    request.serializeTo(err, &generated);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(original.remaining() == generated.remaining());
    CPPUNIT_ASSERT(::memcmp(original.bytes(), generated.bytes(), original.remaining()) == 0);
}

void testAuthenticationResponse() {
    SharedByteBuffer original = fileAsByteBuffer("authentication_response.msg");
    errType err = errOk;
    original.position(err, 4);//skip length prefix
    CPPUNIT_ASSERT(err == errOk);
    AuthenticationResponse response(original);
    CPPUNIT_ASSERT(response.success());
    CPPUNIT_ASSERT(response.hostId() == 0);
    CPPUNIT_ASSERT(response.clusterStartTime() == 1277314297206);
    CPPUNIT_ASSERT(response.leaderAddress() == 2130706433);
    CPPUNIT_ASSERT(response.buildString() == "?revision=");
}

void testInvocationAllParams() {
    SharedByteBuffer original = fileAsByteBuffer("invocation_request_all_params.msg");
    std::vector<Parameter> params;
    params.push_back(Parameter(WIRE_TYPE_STRING, true));
    params.push_back(Parameter(WIRE_TYPE_TINYINT, true));
    params.push_back(Parameter(WIRE_TYPE_SMALLINT, true));
    params.push_back(Parameter(WIRE_TYPE_INTEGER, true));
    params.push_back(Parameter(WIRE_TYPE_BIGINT, true));
    params.push_back(Parameter(WIRE_TYPE_FLOAT, true));
    params.push_back(Parameter(WIRE_TYPE_TIMESTAMP, true));
    params.push_back(Parameter(WIRE_TYPE_DECIMAL, true));
    params.push_back(Parameter(WIRE_TYPE_DECIMAL, true));
    params.push_back(Parameter(WIRE_TYPE_STRING));
    params.push_back(Parameter(WIRE_TYPE_TINYINT));
    params.push_back(Parameter(WIRE_TYPE_SMALLINT));
    params.push_back(Parameter(WIRE_TYPE_INTEGER));
    params.push_back(Parameter(WIRE_TYPE_BIGINT));
    params.push_back(Parameter(WIRE_TYPE_FLOAT));
    params.push_back(Parameter(WIRE_TYPE_TIMESTAMP));
    params.push_back(Parameter(WIRE_TYPE_DECIMAL));
    Procedure proc("foo", params);
    ParameterSet *ps = proc.params();
    errType err = errOk;
    std::vector<std::string> strings; strings.push_back("oh"); strings.push_back("noes"); ps->addString(err, strings);
    CPPUNIT_ASSERT(err == errOk);
    std::vector<int8_t> bytes; bytes.push_back( 22 ); bytes.push_back( 33 ); bytes.push_back( 44 ); ps->addInt8(err, bytes);
    CPPUNIT_ASSERT(err == errOk);
    std::vector<int16_t> shorts; shorts.push_back( 22 ); shorts.push_back( 33 ); shorts.push_back( 44 ); ps->addInt16(err, shorts);
    CPPUNIT_ASSERT(err == errOk);
    std::vector<int32_t> ints; ints.push_back( 22 ); ints.push_back( 33 ); ints.push_back( 44 ); ps->addInt32(err, ints);
    CPPUNIT_ASSERT(err == errOk);
    std::vector<int64_t> longs; longs.push_back( 22 ); longs.push_back( 33 ); longs.push_back( 44 ); ps->addInt64(err, longs);
    CPPUNIT_ASSERT(err == errOk);
    std::vector<double> doubles; doubles.push_back( 3 ); doubles.push_back( 3.1 ); doubles.push_back( 3.14 ); doubles.push_back( 3.1459 ); ps->addDouble(err, doubles);
    CPPUNIT_ASSERT(err == errOk);
    std::vector<int64_t> timestamps; timestamps.push_back(33); timestamps.push_back(44); ps->addTimestamp(err, timestamps);
    CPPUNIT_ASSERT(err == errOk);
    std::vector<Decimal> decimals; decimals.push_back(Decimal(std::string("3"))); decimals.push_back(std::string("3.14")); decimals.push_back(std::string("3.1459")); ps->addDecimal(err, decimals);
    CPPUNIT_ASSERT(err == errOk);
    ps->addNull(err);
    CPPUNIT_ASSERT(err == errOk);
    ps->addString(err, "ohnoes!");
    CPPUNIT_ASSERT(err == errOk);
    ps->addInt8(err, 22);
    CPPUNIT_ASSERT(err == errOk);
    ps->addInt16(err, 22);
    CPPUNIT_ASSERT(err == errOk);
    ps->addInt32(err, 22);
    CPPUNIT_ASSERT(err == errOk);
    ps->addInt64(err, 22);
    CPPUNIT_ASSERT(err == errOk);
    ps->addDouble(err, 3.1459);
    CPPUNIT_ASSERT(err == errOk);
    ps->addTimestamp(err, 33);
    CPPUNIT_ASSERT(err == errOk);
    ps->addDecimal(err, Decimal(std::string("3.1459")));
    CPPUNIT_ASSERT(err == errOk);
    int32_t size = proc.getSerializedSize(err);
    CPPUNIT_ASSERT(err == errOk);
    ScopedByteBuffer buffer(new char[size], size);
    proc.serializeTo(err, &buffer, INT64_MIN);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(original.remaining() == size);
    CPPUNIT_ASSERT(::memcmp(original.bytes(), buffer.bytes(), size) == 0);
}

void testInvocationResponseSuccess() {
    SharedByteBuffer original = fileAsByteBuffer("invocation_response_success.msg");
    errType err = errOk;
    original.position(err, 4);
    CPPUNIT_ASSERT(err == errOk);
    boost::shared_array<char> copy(new char[original.remaining()]);
    original.get(err, copy.get(), original.remaining());
    CPPUNIT_ASSERT(err == errOk);
    InvocationResponse response(copy, original.capacity() - 4);
    CPPUNIT_ASSERT(response.success());
    CPPUNIT_ASSERT(response.clientData() == (-9223372036854775807 - 1));
    CPPUNIT_ASSERT(response.appStatusCode() == -128);
    CPPUNIT_ASSERT(response.appStatusString() == "");
    CPPUNIT_ASSERT(response.statusString() == "");
    CPPUNIT_ASSERT(response.results().size() == 0);
    CPPUNIT_ASSERT(response.clusterRoundTripTime() == 9);
}

void testInvocationResponseFailCV() {
    SharedByteBuffer original = fileAsByteBuffer("invocation_response_fail_cv.msg");
    errType err = errOk;
    original.position(err, 4);
    CPPUNIT_ASSERT(err == errOk);
    boost::shared_array<char> copy(new char[original.remaining()]);
    original.get(err, copy.get(), original.remaining());
    CPPUNIT_ASSERT(err == errOk);
    InvocationResponse response(copy, original.capacity() - 4);
    CPPUNIT_ASSERT(response.failure());
    CPPUNIT_ASSERT(response.statusCode() == STATUS_CODE_GRACEFUL_FAILURE);
    CPPUNIT_ASSERT(response.clientData() == -9223372036854775807);
    CPPUNIT_ASSERT(response.appStatusCode() == -128);
    CPPUNIT_ASSERT(response.appStatusString() == "");
    CPPUNIT_ASSERT(response.statusString().find("VOLTDB ERROR: CONSTRAINT VIOLATION") != std::string::npos);
    CPPUNIT_ASSERT(response.results().size() == 0);
    CPPUNIT_ASSERT(response.clusterRoundTripTime() == 7);
}

void testInvocationResponseSelect() {
    SharedByteBuffer original = fileAsByteBuffer("invocation_response_select.msg");
    errType err = errOk;
    original.position(err, 4);
    CPPUNIT_ASSERT(err == errOk);
    boost::shared_array<char> copy(new char[original.remaining()]);
    original.get(err, copy.get(), original.remaining());
    CPPUNIT_ASSERT(err == errOk);
    InvocationResponse response(copy, original.capacity() - 4);
    CPPUNIT_ASSERT(response.success());
    CPPUNIT_ASSERT(response.clientData() == -9223372036854775806);
    CPPUNIT_ASSERT(response.appStatusCode() == -128);
    CPPUNIT_ASSERT(response.appStatusString() == "");
    CPPUNIT_ASSERT(response.statusString() == "");
    CPPUNIT_ASSERT(response.results().size() == 1);
    CPPUNIT_ASSERT(response.clusterRoundTripTime() == 0);
    Table results = response.results()[0];
    CPPUNIT_ASSERT(results.getStatusCode() == -128);
    CPPUNIT_ASSERT(results.rowCount() == 1);
    CPPUNIT_ASSERT(results.columnCount() == 2);
    std::vector<voltdb::Column> columns = results.columns();
    CPPUNIT_ASSERT(columns.size() == 2);
    CPPUNIT_ASSERT(columns[0].m_name == "HELLO");
    CPPUNIT_ASSERT(columns[0].m_type == WIRE_TYPE_STRING);
    CPPUNIT_ASSERT(columns[1].m_name == "WORLD");
    CPPUNIT_ASSERT(columns[1].m_type == WIRE_TYPE_STRING);
    TableIterator iterator = results.iterator();
    int resultCount = 0;
    while (iterator.hasNext()) {
        Row r = iterator.next(err);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(r.getString(err, 0) == "Hello");
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(r.getString(err, "HELLO") == "Hello");
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(r.getString(err, 1) == "World");
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(r.getString(err, "WORLD") == "World");
        CPPUNIT_ASSERT(err == errOk);
        resultCount++;
    }
    CPPUNIT_ASSERT(resultCount == 1);
}

void testSerializedTable() {
    SharedByteBuffer original = fileAsByteBuffer("serialized_table.bin");
    errType err = errOk;
    original.position(err, 4);
    CPPUNIT_ASSERT(err == errOk);
    Table t(original.slice());
    CPPUNIT_ASSERT(t.columnCount() == 7);
    CPPUNIT_ASSERT(t.rowCount() == 4);
    std::vector<Column> columns = t.columns();
    CPPUNIT_ASSERT(columns.size() == 7);
    CPPUNIT_ASSERT(columns[0].m_name == "column1");
    CPPUNIT_ASSERT(columns[1].m_name == "column2");
    CPPUNIT_ASSERT(columns[2].m_name == "column3");
    CPPUNIT_ASSERT(columns[3].m_name == "column4");
    CPPUNIT_ASSERT(columns[4].m_name == "column5");
    CPPUNIT_ASSERT(columns[5].m_name == "column6");
    CPPUNIT_ASSERT(columns[6].m_name == "column7");
    CPPUNIT_ASSERT(columns[0].m_type == WIRE_TYPE_TINYINT);
    CPPUNIT_ASSERT(columns[1].m_type == WIRE_TYPE_STRING);
    CPPUNIT_ASSERT(columns[2].m_type == WIRE_TYPE_SMALLINT);
    CPPUNIT_ASSERT(columns[3].m_type == WIRE_TYPE_INTEGER);
    CPPUNIT_ASSERT(columns[4].m_type == WIRE_TYPE_BIGINT);
    CPPUNIT_ASSERT(columns[5].m_type == WIRE_TYPE_TIMESTAMP);
    CPPUNIT_ASSERT(columns[6].m_type == WIRE_TYPE_DECIMAL);

    TableIterator ti = t.iterator();
    CPPUNIT_ASSERT(ti.hasNext());
    Row r = ti.next(err);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.columnCount() == 7);

    CPPUNIT_ASSERT(r.isNull(err, 0));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.isNull(err, "column1"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getInt8(err, 0) == INT8_MIN);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getInt8(err, "column1") == INT8_MIN);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(r.isNull(err, 1));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.isNull(err, "column2"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getString(err, 1) == "");
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getString(err, "column2") == "");
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(r.isNull(err, 2));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.isNull(err, "column3"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getInt16(err, 2) == INT16_MIN);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getInt16(err, "column3") == INT16_MIN);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(r.isNull(err, 3));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.isNull(err, "column4"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getInt32(err, 3) == INT32_MIN);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getInt32(err, "column4") == INT32_MIN);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(r.isNull(err, 4));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.isNull(err, "column5"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getInt64(err, 4) == INT64_MIN);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getInt64(err, "column5") == INT64_MIN);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(r.isNull(err, 5));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.isNull(err, "column6"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getTimestamp(err, 5) == INT64_MIN);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getTimestamp(err, "column6") == INT64_MIN);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(r.isNull(err, 6));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.isNull(err, "column7"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getDecimal(err, 6).isNull());
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getDecimal(err, "column7").isNull());
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(ti.hasNext());
    r = ti.next(err);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.columnCount() == 7);

    CPPUNIT_ASSERT(!r.isNull(err, 0));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.isNull(err, "column1"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getInt8(err, 0) == 0);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getInt8(err, "column1") == 0);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(!r.isNull(err, 1));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.isNull(err, "column2"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getString(err, 1) == "");
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getString(err, "column2") == "");
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(!r.isNull(err, 2));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.isNull(err, "column3"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getInt16(err, 2) == 2);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getInt16(err, "column3") == 2);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(!r.isNull(err, 3));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.isNull(err, "column4"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getInt32(err, 3) == 4);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getInt32(err, "column4") == 4);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(!r.isNull(err, 4));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.isNull(err, "column5"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getInt64(err, 4) == 5);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getInt64(err, "column5") == 5);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(!r.isNull(err, 5));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.isNull(err, "column6"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getTimestamp(err, 5) == 44);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getTimestamp(err, "column6") == 44);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(!r.isNull(err, 6));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.isNull(err, "column7"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getDecimal(err, 6).toString() == "3.145900000000");
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getDecimal(err, "column7").toString() == "3.145900000000");
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(ti.hasNext());
    r = ti.next(err);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.columnCount() == 7);

    CPPUNIT_ASSERT(!r.isNull(err, 0));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.isNull(err, "column1"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getInt8(err, 0) == 0);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getInt8(err, "column1") == 0);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(r.isNull(err, 1));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.isNull(err, "column2"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getString(err, 1) == "");
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getString(err, "column2") == "");
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(!r.isNull(err, 2));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.isNull(err, "column3"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getInt16(err, 2) == 2);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getInt16(err, "column3") == 2);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(!r.isNull(err, 3));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.isNull(err, "column4"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getInt32(err, 3) == 4);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getInt32(err, "column4") == 4);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(!r.isNull(err, 4));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.isNull(err, "column5"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getInt64(err, 4) == 5);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getInt64(err, "column5") == 5);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(r.isNull(err, 5));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.isNull(err, "column6"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getTimestamp(err, 5) == INT64_MIN);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getTimestamp(err, "column6") == INT64_MIN);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(r.isNull(err, 6));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.isNull(err, "column7"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getDecimal(err, 6).isNull());
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getDecimal(err, "column7").isNull());
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(ti.hasNext());
    r = ti.next(err);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.columnCount() == 7);

    CPPUNIT_ASSERT(r.isNull(err, 0));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.isNull(err, "column1"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getInt8(err, 0) == INT8_MIN);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getInt8(err, "column1") == INT8_MIN);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(!r.isNull(err, 1));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.isNull(err, "column2"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getString(err, 1) == "woobie");
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getString(err, "column2") == "woobie");
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(r.isNull(err, 2));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.isNull(err, "column3"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getInt16(err, 2) == INT16_MIN);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getInt16(err, "column3") == INT16_MIN);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(r.isNull(err, 3));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.isNull(err, "column4"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getInt32(err, 3) == INT32_MIN);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getInt32(err, "column4") == INT32_MIN);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(r.isNull(err, 4));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.isNull(err, "column5"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getInt64(err, 4) == INT64_MIN);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getInt64(err, "column5") == INT64_MIN);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(!r.isNull(err, 5));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.isNull(err, "column6"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getTimestamp(err, 5) == 44);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getTimestamp(err, "column6") == 44);
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(!r.isNull(err, 6));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.isNull(err, "column7"));
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(r.getDecimal(err, 6).toString() == "3.145900000000");
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getDecimal(err, "column7").toString() == "3.145900000000");
    CPPUNIT_ASSERT(err == errOk);
    CPPUNIT_ASSERT(!r.wasNull());
}
};

CPPUNIT_TEST_SUITE_REGISTRATION( SerializationTest );
}
