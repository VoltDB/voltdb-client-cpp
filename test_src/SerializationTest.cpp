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

#include <cppunit/TestCase.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestFixture.h>
#include <boost/scoped_ptr.hpp>
#include <iostream>
#include <exception>
#include <cstdio>
#include "Exception.hpp"
#include "ByteBuffer.hpp"
#include "ClientConfig.h"
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
#include "sha256.h"
#include "Geography.h"
#include "GeographyPoint.h"

namespace voltdb {
class SerializationTest : public CppUnit::TestFixture {
CPPUNIT_TEST_SUITE( SerializationTest );
CPPUNIT_TEST(testAuthenticationRequestSha1);
CPPUNIT_TEST(testAuthenticationRequestSha256);
CPPUNIT_TEST(testAuthenticationResponse);
CPPUNIT_TEST(testInvocationAllParams);
CPPUNIT_TEST(testInvocationResponseSuccess);
CPPUNIT_TEST(testInvocationResponseFailCV);
CPPUNIT_TEST(testInvocationResponseSelect);
CPPUNIT_TEST(testInvocationGeoInsert);
CPPUNIT_TEST(testInvocationGeoInsertNulls);
CPPUNIT_TEST(testInvocationGeoSelectBoth);
CPPUNIT_TEST(testInvocationGeoSelectBothNull);
CPPUNIT_TEST(testInvocationGeoSelectPolyNull);
CPPUNIT_TEST(testInvocationGeoSelectPointNull);

CPPUNIT_TEST(testSerializedTable);
CPPUNIT_TEST_SUITE_END();
public:

    static const long        FAKE_CLUSTER_START_TIME      = 0x4B1DFA11FEEDFACEL;
    static const long        FAKE_CLIENT_DATA             = 0xDEADBEEFDABBAD00L;
    static const long        FAKE_LEADER_IP_ADDRESS       = 0x7f000001;
    static const int         FAKE_CLUSTER_ROUND_TRIP_TIME = 0x00000004;
    static const std::string FAKE_BUILD_STRING;

SharedByteBuffer fileAsByteBuffer(std::string filename) {

    int ret = 0;

    std::string path = "test_src/test_data/" + filename;
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

void testAuthenticationRequestSha1() {
    SharedByteBuffer original = fileAsByteBuffer("authentication_request.msg");
    SharedByteBuffer generated(new char[8192], 8192);
    unsigned char hashedPassword[20];
    std::string password("world");
    SHA1_CTX context;
    SHA1_Init(&context);
    SHA1_Update( &context, reinterpret_cast<const unsigned char*>(password.data()), password.size());
    SHA1_Final ( &context, hashedPassword);
    AuthenticationRequest request( "hello", "database", hashedPassword, HASH_SHA1 );
    request.serializeTo(&generated);
    CPPUNIT_ASSERT(original.remaining() == generated.remaining());
    CPPUNIT_ASSERT(::memcmp(original.bytes(), generated.bytes(), original.remaining()) == 0);
}

void testAuthenticationRequestSha256() {
    SharedByteBuffer original = fileAsByteBuffer("authentication_request_sha256.msg");
    SharedByteBuffer generated(new char[8192], 8192);
    unsigned char hashedPassword[32];
    std::string password("world");
    computeSHA256(password.c_str(), password.size(), hashedPassword);
    AuthenticationRequest request( "hello", "database", hashedPassword, HASH_SHA256);
    request.serializeTo(&generated);
    CPPUNIT_ASSERT(original.remaining() == generated.remaining());
    CPPUNIT_ASSERT(::memcmp(original.bytes(), generated.bytes(), original.remaining()) == 0);
}

void testAuthenticationResponse() {
    SharedByteBuffer original = fileAsByteBuffer("authentication_response.msg");
    original.position(4);//skip length prefix
    AuthenticationResponse response(original);
    CPPUNIT_ASSERT(response.success());
    CPPUNIT_ASSERT(response.hostId()           == 0);
    CPPUNIT_ASSERT(response.clusterStartTime() == FAKE_CLUSTER_START_TIME);
    CPPUNIT_ASSERT(response.leaderAddress()    == FAKE_LEADER_IP_ADDRESS);
    CPPUNIT_ASSERT(response.buildString()      == FAKE_BUILD_STRING);
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
    std::vector<std::string> strings; strings.push_back("oh"); strings.push_back("noes"); ps->addString(strings);
    std::vector<int8_t> bytes; bytes.push_back( 22 ); bytes.push_back( 33 ); bytes.push_back( 44 ); ps->addInt8(bytes);
    std::vector<int16_t> shorts; shorts.push_back( 22 ); shorts.push_back( 33 ); shorts.push_back( 44 ); ps->addInt16(shorts);
    std::vector<int32_t> ints; ints.push_back( 22 ); ints.push_back( 33 ); ints.push_back( 44 ); ps->addInt32(ints);
    std::vector<int64_t> longs; longs.push_back( 22 ); longs.push_back( 33 ); longs.push_back( 44 ); ps->addInt64(longs);
    std::vector<double> doubles; doubles.push_back( 3 ); doubles.push_back( 3.1 ); doubles.push_back( 3.14 ); doubles.push_back( 3.1459 ); ps->addDouble(doubles);
    std::vector<int64_t> timestamps; timestamps.push_back(33); timestamps.push_back(44); ps->addTimestamp(timestamps);
    std::vector<Decimal> decimals; decimals.push_back(Decimal(std::string("3"))); decimals.push_back(std::string("3.14")); decimals.push_back(std::string("3.1459")); ps->addDecimal(decimals);
    ps->addNull().addString("ohnoes!").addInt8(22).addInt16(22).addInt32(22).addInt64(22).addDouble(3.1459).addTimestamp(33).addDecimal(Decimal(std::string("3.1459")));
    int32_t size = proc.getSerializedSize();
    ScopedByteBuffer buffer(new char[size], size);
    proc.serializeTo(&buffer, FAKE_CLIENT_DATA);
    int cmp = ::memcmp(original.bytes(), buffer.bytes(), size);
#if  0
    // Dump the request for debugging.
    {
        uint8_t genbuf[1024];
        buffer.get(0, (char *)genbuf, buffer.limit());
        std::ofstream of("generated_invocation_request_all_params.msg");
        of.write((const char *)genbuf, buffer.limit());
        of.close();
    }
    std::cout << "Comparison: " << cmp << "\n";
    if (cmp != 0) {
        int width = std::cout.width();
        char *orig_bytes = original.bytes();
        char *buff_bytes = buffer.bytes();
        for (int idx = 0; idx < size; idx += 1) {
            if (orig_bytes[idx] != buff_bytes[idx]) {
                std::cout << "   Byte " << idx << " differs: "
                          << std::hex << (((unsigned int)orig_bytes[idx]) & 0xff) << std::dec
                          << " != "
                          << std::hex << (((unsigned int)buff_bytes[idx]) & 0xff) << std::dec << "\n";
            }
        }
        std::cout.width(width);
    }
#endif
    CPPUNIT_ASSERT(original.remaining() == size);
    CPPUNIT_ASSERT(cmp == 0);
}

void testInvocationResponseSuccess() {
    SharedByteBuffer original = fileAsByteBuffer("invocation_response_success.msg");
    original.position(4);
    boost::shared_array<char> copy(new char[original.remaining()]);
    original.get(copy.get(), original.remaining());
    InvocationResponse response(copy, original.capacity() - 4);
    CPPUNIT_ASSERT(response.success());
    CPPUNIT_ASSERT(response.clientData() == FAKE_CLIENT_DATA);
    CPPUNIT_ASSERT(response.appStatusCode() == -128);
    CPPUNIT_ASSERT(response.appStatusString() == "");
    CPPUNIT_ASSERT(response.statusString() == "");
    CPPUNIT_ASSERT(response.results().size() == 1);
    CPPUNIT_ASSERT(response.clusterRoundTripTime() == FAKE_CLUSTER_ROUND_TRIP_TIME);
}

void testInvocationResponseFailCV() {
    SharedByteBuffer original = fileAsByteBuffer("invocation_response_fail_cv.msg");
    original.position(4);
    boost::shared_array<char> copy(new char[original.remaining()]);
    original.get(copy.get(), original.remaining());
    InvocationResponse response(copy, original.capacity() - 4);
    CPPUNIT_ASSERT(response.failure());
    CPPUNIT_ASSERT(response.statusCode() == STATUS_CODE_GRACEFUL_FAILURE);
    CPPUNIT_ASSERT(response.clientData() == FAKE_CLIENT_DATA);
    CPPUNIT_ASSERT(response.appStatusCode() == -128);
    CPPUNIT_ASSERT(response.appStatusString() == "");
    CPPUNIT_ASSERT(response.statusString().find("VOLTDB ERROR: CONSTRAINT VIOLATION") != std::string::npos);
    CPPUNIT_ASSERT(response.results().size() == 0);
    CPPUNIT_ASSERT(response.clusterRoundTripTime() == FAKE_CLUSTER_ROUND_TRIP_TIME);
}

void testInvocationResponseSelect() {
    SharedByteBuffer original = fileAsByteBuffer("invocation_response_select.msg");
    original.position(4);
    boost::shared_array<char> copy(new char[original.remaining()]);
    original.get(copy.get(), original.remaining());
    InvocationResponse response(copy, original.capacity() - 4);
    CPPUNIT_ASSERT(response.success());
    CPPUNIT_ASSERT(response.clientData() == FAKE_CLIENT_DATA);
    CPPUNIT_ASSERT(response.appStatusCode() == -128);
    CPPUNIT_ASSERT(response.appStatusString() == "");
    CPPUNIT_ASSERT(response.statusString() == "");
    CPPUNIT_ASSERT(response.results().size() == 1);
    CPPUNIT_ASSERT(response.clusterRoundTripTime() == FAKE_CLUSTER_ROUND_TRIP_TIME);
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
        Row r = iterator.next();
        CPPUNIT_ASSERT(r.getString(0) == "Hello");
        CPPUNIT_ASSERT(r.getString("HELLO") == "Hello");
        CPPUNIT_ASSERT(r.getString(1) == "World");
        CPPUNIT_ASSERT(r.getString("WORLD") == "World");
        resultCount++;
    }
    CPPUNIT_ASSERT(resultCount == 1);
}

void testInvocationGeoInsert() {
    SharedByteBuffer original = fileAsByteBuffer("invocation_response_success.msg");
    original.position(4);
    boost::shared_array<char> copy(new char[original.remaining()]);
    original.get(copy.get(), original.remaining());
    InvocationResponse response(copy, original.capacity() - 4);
    CPPUNIT_ASSERT(response.success());
    CPPUNIT_ASSERT(response.clientData() == FAKE_CLIENT_DATA);
    CPPUNIT_ASSERT(response.appStatusCode() == -128);
    CPPUNIT_ASSERT(response.appStatusString() == "");
    CPPUNIT_ASSERT(response.statusString() == "");
    CPPUNIT_ASSERT(response.results().size() == 1);
    CPPUNIT_ASSERT(response.clusterRoundTripTime() == FAKE_CLUSTER_ROUND_TRIP_TIME);
}

void testInvocationGeoInsertNulls() {
    SharedByteBuffer original = fileAsByteBuffer("invocation_response_success.msg");
    original.position(4);
    boost::shared_array<char> copy(new char[original.remaining()]);
    original.get(copy.get(), original.remaining());
    InvocationResponse response(copy, original.capacity() - 4);
    CPPUNIT_ASSERT(response.success());
    CPPUNIT_ASSERT(response.clientData() == FAKE_CLIENT_DATA);
    CPPUNIT_ASSERT(response.appStatusCode() == -128);
    CPPUNIT_ASSERT(response.appStatusString() == "");
    CPPUNIT_ASSERT(response.statusString() == "");
    CPPUNIT_ASSERT(response.results().size() == 1);
    CPPUNIT_ASSERT(response.clusterRoundTripTime() == FAKE_CLUSTER_ROUND_TRIP_TIME);
}

void testInvocationGeoSelectBoth() {
    SharedByteBuffer original = fileAsByteBuffer("invocation_response_select_geo_both.msg");
    original.position(4);
    boost::shared_array<char> copy(new char[original.remaining()]);
    original.get(copy.get(), original.remaining());
    InvocationResponse response(copy, original.capacity() - 4);
    CPPUNIT_ASSERT(response.success());
    CPPUNIT_ASSERT(response.clientData() == FAKE_CLIENT_DATA);
    CPPUNIT_ASSERT(response.appStatusCode() == -128);
    CPPUNIT_ASSERT(response.appStatusString() == "");
    CPPUNIT_ASSERT(response.statusString() == "");
    CPPUNIT_ASSERT(response.results().size() == 1);
    CPPUNIT_ASSERT(response.clusterRoundTripTime() == FAKE_CLUSTER_ROUND_TRIP_TIME);
    Table results = response.results()[0];
    CPPUNIT_ASSERT(results.getStatusCode() == -128);
    CPPUNIT_ASSERT(results.rowCount() == 1);
    CPPUNIT_ASSERT(results.columnCount() == 3);
    std::vector<voltdb::Column> columns = results.columns();
    CPPUNIT_ASSERT(columns.size() == 3);
    CPPUNIT_ASSERT(columns[0].m_name == "ID");
    CPPUNIT_ASSERT(columns[0].m_type == WIRE_TYPE_BIGINT);
    CPPUNIT_ASSERT(columns[1].m_name == "GEO");
    CPPUNIT_ASSERT(columns[1].m_type == WIRE_TYPE_GEOGRAPHY);
    CPPUNIT_ASSERT(columns[2].m_name == "GEO_PT");
    CPPUNIT_ASSERT(columns[2].m_type == WIRE_TYPE_GEOGRAPHY_POINT);
    TableIterator iterator = results.iterator();
    int resultCount = 0;
    GeographyPoint smallPoint(0.5, 0.5);
    GeographyPoint originPoint(0, 0);
    Geography smallPoly;
    Geography midPoly;
    Geography::Ring rng;
    smallPoly << (rng << GeographyPoint(0, 0)
                      << GeographyPoint(1, 0)
                      << GeographyPoint(1, 1)
                      << GeographyPoint(0, 1)
                      << GeographyPoint(0, 0));
    Geography::Ring rng2;
    midPoly << (rng2 << GeographyPoint(0, 0)
                     << GeographyPoint(45, 0)
                     << GeographyPoint(45, 45)
                     << GeographyPoint(0, 45)
                     << GeographyPoint(0, 0));
    while (iterator.hasNext()) {
        Row r = iterator.next();
        CPPUNIT_ASSERT(r.columnCount() == 3);
        CPPUNIT_ASSERT(r.getInt64(0) == 100L);
        CPPUNIT_ASSERT(r.getInt64("ID") == 100L);
        CPPUNIT_ASSERT(r.getGeography("GEO") == smallPoly);
        CPPUNIT_ASSERT(r.getGeography(1) == smallPoly);
        CPPUNIT_ASSERT(r.getGeographyPoint(2) == originPoint);
        CPPUNIT_ASSERT(r.getGeographyPoint("GEO_PT") == originPoint);
        resultCount++;
    }
    CPPUNIT_ASSERT(resultCount == 1);
}

void testInvocationGeoSelectPolyNull() {

}

void testInvocationGeoSelectPointNull() {

}

void testInvocationGeoSelectBothNull() {

}

void testSerializedTable() {
    SharedByteBuffer original = fileAsByteBuffer("serialized_table.bin");
    original.position(4);
    Table t(original.slice());
    CPPUNIT_ASSERT(t.columnCount() == 9);
    CPPUNIT_ASSERT(t.rowCount() == 4);
    std::vector<Column> columns = t.columns();
    CPPUNIT_ASSERT(columns.size() == 9);
    CPPUNIT_ASSERT(columns[0].m_name == "column1");
    CPPUNIT_ASSERT(columns[1].m_name == "column2");
    CPPUNIT_ASSERT(columns[2].m_name == "column3");
    CPPUNIT_ASSERT(columns[3].m_name == "column4");
    CPPUNIT_ASSERT(columns[4].m_name == "column5");
    CPPUNIT_ASSERT(columns[5].m_name == "column6");
    CPPUNIT_ASSERT(columns[6].m_name == "column7");
    CPPUNIT_ASSERT(columns[7].m_name == "column8");
    CPPUNIT_ASSERT(columns[8].m_name == "column9");
    CPPUNIT_ASSERT(columns[0].m_type == WIRE_TYPE_TINYINT);
    CPPUNIT_ASSERT(columns[1].m_type == WIRE_TYPE_STRING);
    CPPUNIT_ASSERT(columns[2].m_type == WIRE_TYPE_SMALLINT);
    CPPUNIT_ASSERT(columns[3].m_type == WIRE_TYPE_INTEGER);
    CPPUNIT_ASSERT(columns[4].m_type == WIRE_TYPE_BIGINT);
    CPPUNIT_ASSERT(columns[5].m_type == WIRE_TYPE_TIMESTAMP);
    CPPUNIT_ASSERT(columns[6].m_type == WIRE_TYPE_DECIMAL);
    CPPUNIT_ASSERT(columns[7].m_type == WIRE_TYPE_GEOGRAPHY);
    CPPUNIT_ASSERT(columns[8].m_type == WIRE_TYPE_GEOGRAPHY_POINT);

    TableIterator ti = t.iterator();
    CPPUNIT_ASSERT(ti.hasNext());
    Row r = ti.next();
    CPPUNIT_ASSERT(r.columnCount() == 9);

    CPPUNIT_ASSERT(r.isNull(0));
    CPPUNIT_ASSERT(r.isNull("column1"));
    CPPUNIT_ASSERT(r.getInt8(0) == INT8_MIN);
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getInt8("column1") == INT8_MIN);
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(r.isNull(1));
    CPPUNIT_ASSERT(r.isNull("column2"));
    CPPUNIT_ASSERT(r.getString(1) == "");
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getString("column2") == "");
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(r.isNull(2));
    CPPUNIT_ASSERT(r.isNull("column3"));
    CPPUNIT_ASSERT(r.getInt16(2) == INT16_MIN);
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getInt16("column3") == INT16_MIN);
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(r.isNull(3));
    CPPUNIT_ASSERT(r.isNull("column4"));
    CPPUNIT_ASSERT(r.getInt32(3) == INT32_MIN);
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getInt32("column4") == INT32_MIN);
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(r.isNull(4));
    CPPUNIT_ASSERT(r.isNull("column5"));
    CPPUNIT_ASSERT(r.getInt64(4) == INT64_MIN);
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getInt64("column5") == INT64_MIN);
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(r.isNull(5));
    CPPUNIT_ASSERT(r.isNull("column6"));
    CPPUNIT_ASSERT(r.getTimestamp(5) == INT64_MIN);
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getTimestamp("column6") == INT64_MIN);
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(r.isNull(6));
    CPPUNIT_ASSERT(r.isNull("column7"));
    CPPUNIT_ASSERT(r.getDecimal(6).isNull());
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getDecimal("column7").isNull());
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(ti.hasNext());
    r = ti.next();
    CPPUNIT_ASSERT(r.columnCount() == 7);

    CPPUNIT_ASSERT(!r.isNull(0));
    CPPUNIT_ASSERT(!r.isNull("column1"));
    CPPUNIT_ASSERT(r.getInt8(0) == 0);
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getInt8("column1") == 0);
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(!r.isNull(1));
    CPPUNIT_ASSERT(!r.isNull("column2"));
    CPPUNIT_ASSERT(r.getString(1) == "");
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getString("column2") == "");
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(!r.isNull(2));
    CPPUNIT_ASSERT(!r.isNull("column3"));
    CPPUNIT_ASSERT(r.getInt16(2) == 2);
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getInt16("column3") == 2);
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(!r.isNull(3));
    CPPUNIT_ASSERT(!r.isNull("column4"));
    CPPUNIT_ASSERT(r.getInt32(3) == 4);
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getInt32("column4") == 4);
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(!r.isNull(4));
    CPPUNIT_ASSERT(!r.isNull("column5"));
    CPPUNIT_ASSERT(r.getInt64(4) == 5);
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getInt64("column5") == 5);
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(!r.isNull(5));
    CPPUNIT_ASSERT(!r.isNull("column6"));
    CPPUNIT_ASSERT(r.getTimestamp(5) == 44);
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getTimestamp("column6") == 44);
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(!r.isNull(6));
    CPPUNIT_ASSERT(!r.isNull("column7"));
    CPPUNIT_ASSERT(r.getDecimal(6).toString() == "3.145900000000");
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getDecimal("column7").toString() == "3.145900000000");
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(ti.hasNext());
    r = ti.next();
    CPPUNIT_ASSERT(r.columnCount() == 7);

    CPPUNIT_ASSERT(!r.isNull(0));
    CPPUNIT_ASSERT(!r.isNull("column1"));
    CPPUNIT_ASSERT(r.getInt8(0) == 0);
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getInt8("column1") == 0);
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(r.isNull(1));
    CPPUNIT_ASSERT(r.isNull("column2"));
    CPPUNIT_ASSERT(r.getString(1) == "");
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getString("column2") == "");
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(!r.isNull(2));
    CPPUNIT_ASSERT(!r.isNull("column3"));
    CPPUNIT_ASSERT(r.getInt16(2) == 2);
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getInt16("column3") == 2);
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(!r.isNull(3));
    CPPUNIT_ASSERT(!r.isNull("column4"));
    CPPUNIT_ASSERT(r.getInt32(3) == 4);
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getInt32("column4") == 4);
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(!r.isNull(4));
    CPPUNIT_ASSERT(!r.isNull("column5"));
    CPPUNIT_ASSERT(r.getInt64(4) == 5);
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getInt64("column5") == 5);
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(r.isNull(5));
    CPPUNIT_ASSERT(r.isNull("column6"));
    CPPUNIT_ASSERT(r.getTimestamp(5) == INT64_MIN);
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getTimestamp("column6") == INT64_MIN);
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(r.isNull(6));
    CPPUNIT_ASSERT(r.isNull("column7"));
    CPPUNIT_ASSERT(r.getDecimal(6).isNull());
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getDecimal("column7").isNull());
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(ti.hasNext());
    r = ti.next();
    CPPUNIT_ASSERT(r.columnCount() == 7);

    CPPUNIT_ASSERT(r.isNull(0));
    CPPUNIT_ASSERT(r.isNull("column1"));
    CPPUNIT_ASSERT(r.getInt8(0) == INT8_MIN);
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getInt8("column1") == INT8_MIN);
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(!r.isNull(1));
    CPPUNIT_ASSERT(!r.isNull("column2"));
    CPPUNIT_ASSERT(r.getString(1) == "woobie");
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getString("column2") == "woobie");
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(r.isNull(2));
    CPPUNIT_ASSERT(r.isNull("column3"));
    CPPUNIT_ASSERT(r.getInt16(2) == INT16_MIN);
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getInt16("column3") == INT16_MIN);
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(r.isNull(3));
    CPPUNIT_ASSERT(r.isNull("column4"));
    CPPUNIT_ASSERT(r.getInt32(3) == INT32_MIN);
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getInt32("column4") == INT32_MIN);
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(r.isNull(4));
    CPPUNIT_ASSERT(r.isNull("column5"));
    CPPUNIT_ASSERT(r.getInt64(4) == INT64_MIN);
    CPPUNIT_ASSERT(r.wasNull());
    CPPUNIT_ASSERT(r.getInt64("column5") == INT64_MIN);
    CPPUNIT_ASSERT(r.wasNull());

    CPPUNIT_ASSERT(!r.isNull(5));
    CPPUNIT_ASSERT(!r.isNull("column6"));
    CPPUNIT_ASSERT(r.getTimestamp(5) == 44);
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getTimestamp("column6") == 44);
    CPPUNIT_ASSERT(!r.wasNull());

    CPPUNIT_ASSERT(!r.isNull(6));
    CPPUNIT_ASSERT(!r.isNull("column7"));
    CPPUNIT_ASSERT(r.getDecimal(6).toString() == "3.145900000000");
    CPPUNIT_ASSERT(!r.wasNull());
    CPPUNIT_ASSERT(r.getDecimal("column7").toString() == "3.145900000000");
    CPPUNIT_ASSERT(!r.wasNull());
}
};

const std::string SerializationTest::FAKE_BUILD_STRING("volt_6.1_test_build_string");

CPPUNIT_TEST_SUITE_REGISTRATION( SerializationTest );
}
