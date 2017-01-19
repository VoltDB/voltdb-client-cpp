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
#include "RowBuilder.h"
#include "Row.hpp"
#include "sha1.h"
#include "sha256.h"
#include "Geography.hpp"
#include "GeographyPoint.hpp"

#undef DEBUG_BYTE_BUFFERS
#ifdef  DEBUG_BYTE_BUFFERS
#include <ctype.h>
#endif

namespace voltdb {

static const double EPSILON = 1.0e-14;

void dumpBytes(char *bytes, size_t length) {
    for (int i = 0; i < length; ++i) {
        printf("%02x ", bytes[i] && 0xff);
        if (i % 16 == 0) {
            printf("\n");
        }
    }
    printf("\n");
}


void compareBuffers(char *first, char*second, size_t length) {
    for (int i = 0; i < length; ++i) {
        if (first[i] != second[i]) {
            printf("%d (%02x %02x) ", i, (first[i] & 0xff), (second[i] & 0xff));
        }
    }
    printf("\n");
}

static void compareByteBuffers(ByteBuffer &original, std::string originalName,
                               ByteBuffer generated, std::string generatedName) {
    int orig_size = original.remaining();
    int gen_size  = generated.remaining();
    int size = (orig_size < gen_size) ? orig_size : gen_size;
    int cmp = ::memcmp(original.bytes(), generated.bytes(), size);
#ifdef DEBUG_BYTE_BUFFERS
    if (gen_size != orig_size || cmp != 0) {
        char buffer[8192];
        std::ofstream gen_file(generatedName);
        std::ofstream orig_file(originalName);
        generated.get(0, buffer, generated.remaining());
        gen_file.write(buffer, generated.remaining());
        gen_file.close();
        original.get(0, buffer, original.remaining());
        orig_file.write(buffer, original.remaining());
        orig_file.close();
        std::cout << "Byte buffer comparison failure: cmp == " << cmp << "\n";
        std::cout << "                                original size == " << orig_size
                  << ", generated size == " << gen_size << "\n";
        std::cout << "Idx   Original  Generated\n";
        for (int idx = 0; idx < size; idx += 1) {
            char buff1[8];
            char buff2[8];
            unsigned char oval = (unsigned char)original.getInt8(idx);
            unsigned char gval = (unsigned char)generated.getInt8(idx);
            printf("%04d.) %5d (%s) %5d (%s)%s\n",
                   idx,
                   oval,
                   (sprintf(buff1,
                            isprint(oval) ? "%2c" : "%02x",
                            oval), buff1),
                   gval,
                   (sprintf(buff2,
                            isprint(gval) ? "%2c" : "%02x",
                            gval), buff2),
                   (oval != gval) ? "***" : "");
        }
    }
#endif
    CPPUNIT_ASSERT(gen_size == orig_size);
    CPPUNIT_ASSERT(cmp == 0);
}

static bool getParameterType(ByteBuffer &message, int8_t &type, bool &isArray) {
    isArray = false;
    type = message.getInt8();
    if (type == WIRE_TYPE_ARRAY) {
        type = message.getInt8();
        if (type == WIRE_TYPE_TINYINT) {
            type = WIRE_TYPE_VARBINARY;
        } else {
            isArray = true;
        }
    }
    return true;
}

static bool compareParameter(ByteBuffer &original, std::string originalName,
                             ByteBuffer &generated, std::string generatedName,
                             bool allowArrays = true)
{
    int8_t oParamType, gParamType;
    bool   oIsArray, gIsArray;
    if (!getParameterType(original, oParamType, oIsArray)) {
        return false;
    }
    if (!getParameterType(generated, gParamType, gIsArray)) {
        return false;
    }
    if ((oIsArray != gIsArray) || (oParamType != gParamType)) {
        return false;
    }
    if (oIsArray) {
        if (allowArrays) {
            return false;
        }
        int32_t gNumElements = generated.getInt16();
        int32_t oNumElements = original.getInt16();
        if (gNumElements != oNumElements) {
            return false;
        }
        for (int idx = 0; idx < gNumElements; idx += 1) {
            if (!compareParameter(original, originalName,
                                  generated, generatedName, false)) {
                return false;
            }
        }
        return true;
    }
    switch (oParamType) {
    case WIRE_TYPE_STRING:
    case WIRE_TYPE_VARBINARY:
        {
            int32_t gsize = generated.getInt32();
            int32_t osize = original.getInt32();
            if (gsize != osize) {
                return false;
            }
            bool oWasNull = false;
            bool gWasNull = false;
            std::string ostr = original.getString(oWasNull);
            std::string gstr = original.getString(gWasNull);
            if (oWasNull != gWasNull) {
                return false;
            }
            if (ostr != gstr) {
                return false;
            }
            return true;
        }
        break;
    case WIRE_TYPE_GEOGRAPHY:
        {
            bool oWasNull = false;
            bool gWasNull = false;
            Geography ogeog;
            int32_t oGSize = ogeog.deserializeFrom(original,
                                                   original.position(),
                                                   oWasNull);
            Geography ggeog;
            int32_t gGSize = ggeog.deserializeFrom(generated,
                                                   generated.position(),
                                                   gWasNull);
            if (oGSize != gGSize) {
                return false;
            }
            if (!ogeog.approximatelyEqual(ggeog, EPSILON)) {
                return false;
            }
            return true;
        }
        break;
    case WIRE_TYPE_TINYINT:
        {
            int8_t g = generated.getInt8();
            int8_t o = original.getInt8();
            return (g == o);
        }
    case WIRE_TYPE_SMALLINT:
        {
            int16_t g = generated.getInt16();
            int16_t o = original.getInt16();
            return (g == o);
        }
    case WIRE_TYPE_INTEGER:
        {
            int32_t g = generated.getInt32();
            int32_t o = original.getInt32();
            return (g == o);
        }
    case WIRE_TYPE_BIGINT:
        {
            int64_t g = generated.getInt64();
            int64_t o = original.getInt64();
            return (g == o);
        }
    case WIRE_TYPE_FLOAT:
        {
            double g = generated.getDouble();
            double o = original.getDouble();
            return (g == o);
        }
    case WIRE_TYPE_TIMESTAMP:
        {
            int64_t g = generated.getInt64();
            int64_t o = original.getInt64();
            return (g == o);
        }
    case WIRE_TYPE_DECIMAL:
        {
            int64_t g = generated.getInt64();
            int64_t o = original.getInt64();
            return (g == o);
        }
    case WIRE_TYPE_GEOGRAPHY_POINT:
        {
            GeographyPoint g, o;
            bool gWasNull = false;
            bool oWasNull = false;
            int32_t oSize = o.deserializeFrom(original,
                                              original.position(),
                                              oWasNull);
            int32_t gSize = g.deserializeFrom(generated,
                                              generated.position(),
                                              gWasNull);
            if (gSize != oSize) {
                return false;
            }
            original.position(original.position() + oSize);
            generated.position(generated.position() + gSize);
            return g.approximatelyEqual(o, EPSILON);
        }
    default:
        return false;
    }
}

static bool compareParameterSets(ByteBuffer &original, std::string originalName,
                                 ByteBuffer &generated, std::string generatedName)
{
    int16_t oNumParams = original.getInt16();
    int16_t gNumParams = generated.getInt16();
    if (oNumParams != gNumParams) {
        return false;
    }
    for (int idx = 0; idx < oNumParams; idx += 1) {
        if (!compareParameter(original, originalName,
                              generated, generatedName)) {
            return false;
        }
    }
    return true;
}

static WireType schemaColTypes[] = {
        WIRE_TYPE_TINYINT,
        WIRE_TYPE_STRING,
        WIRE_TYPE_SMALLINT,
        WIRE_TYPE_INTEGER,
        WIRE_TYPE_BIGINT,
        WIRE_TYPE_TIMESTAMP,
        WIRE_TYPE_DECIMAL,
        WIRE_TYPE_GEOGRAPHY,
        WIRE_TYPE_GEOGRAPHY_POINT
};

void generateTestSchema(std::vector<Column> &columns) {
    if (!columns.empty()) {
        columns.clear();
    }

    char columnNames[32];
    for(int i = 0; i < sizeof(schemaColTypes)/sizeof(schemaColTypes[0]); ++i) {
        snprintf(columnNames, sizeof(columnNames), "column%d", i + 1);
        columns.push_back(voltdb::Column(columnNames, schemaColTypes[i]));
    }
}

/**
 * Reset the position of a byte buffer to its initial position.
 */
struct BBReset {
    BBReset(ByteBuffer &bb)
        : m_bb(bb), m_initPos(bb.position()) {}
    ~BBReset() {
        m_bb.position(m_initPos);
    }
    ByteBuffer &m_bb;
    int m_initPos;
};

static bool compareRequestMessages(ByteBuffer &original, std::string originalName,
                                   ByteBuffer &generated, std::string generatedName)
{
    BBReset oreset(original);
    BBReset greset(generated);
    int32_t osize = original.getInt32();
    int32_t gsize = generated.getInt32();
    if (osize != gsize) {
        return false;
    } else {
        int8_t  oprotocol = original.getInt8();
        int8_t  gprotocol = generated.getInt8();
        if (oprotocol != gprotocol) {
            return false;
        }
        bool oWasNull;
        bool gWasNull;
        std::string oProcName = original.getString(oWasNull);
        std::string gProcName = generated.getString(gWasNull);
        if (oProcName != oProcName || (oWasNull != gWasNull)) {
            return false;
        }
        int64_t oClientData = original.getInt64();
        int64_t gClientData = generated.getInt64();
        if (oClientData != gClientData) {
            return false;
        }
        return compareParameterSets(original, originalName,
                                    generated, generatedName);
    }
}

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
CPPUNIT_TEST(testTableSerialization);

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
    CPPUNIT_ASSERT(response.getHostId()           == 0);
    CPPUNIT_ASSERT(response.getClusterStartTime() == FAKE_CLUSTER_START_TIME);
    CPPUNIT_ASSERT(response.getLeaderAddress()    == FAKE_LEADER_IP_ADDRESS);
    CPPUNIT_ASSERT(response.getBuildString()      == FAKE_BUILD_STRING);
}

/**
 * Here we are going to create a procedure which mimics
 * the java client's procedure which we have already generated
 * and captured.  We will then compare the two messages.
 * We have already captured the response, so we validate that
 * as well.
 */
void testInvocationGeoInsert() {
    // This is the message the Java client generated.
    SharedByteBuffer originalRequest = fileAsByteBuffer("invocation_request_insert_geo.msg");
    // This is the the response the Java client generated.
    SharedByteBuffer originalResponse = fileAsByteBuffer("invocation_response_insert_geo.msg");

    std::vector<Parameter> params;
    params.push_back(Parameter(WIRE_TYPE_INTEGER, false));
    params.push_back(Parameter(WIRE_TYPE_GEOGRAPHY, false));
    params.push_back(Parameter(WIRE_TYPE_GEOGRAPHY_POINT, false));
    Procedure proc("InsertGeo", params);
    ParameterSet *ps = proc.params();
    Geography smallPoly;
    smallPoly.addEmptyRing()
        << GeographyPoint(0, 0)
        << GeographyPoint(1, 0)
        << GeographyPoint(1, 1)
        << GeographyPoint(0, 1)
        << GeographyPoint(0, 0);
    smallPoly.addEmptyRing()
        << GeographyPoint(0.1, 0.1)
        << GeographyPoint(0.1, 0.9)
        << GeographyPoint(0.9, 0.9)
        << GeographyPoint(0.9, 0.1)
        << GeographyPoint(0.1, 0.1);
    GeographyPoint smallPoint(0.5, 0.5);
    ps->addInt32(200);
    ps->addGeography(smallPoly);
    ps->addGeographyPoint(smallPoint);
    int32_t procSize = proc.getSerializedSize();
    ScopedByteBuffer generatedRequest(procSize);
    proc.serializeTo(&generatedRequest, FAKE_CLIENT_DATA);

    // compare originalRequest and generatedRequest.
    compareRequestMessages(originalRequest,  "original_insert_geo.msg",
                           generatedRequest, "generated_insert_geo.msg");
    
    // Investigate the response.  This is less interesting.
    originalResponse.position(4);
    boost::shared_array<char> copy(new char[originalResponse.remaining()]);
    originalResponse.get(copy.get(), originalResponse.remaining());
    InvocationResponse response(copy, originalResponse.capacity() - 4);
    CPPUNIT_ASSERT(response.success());
    CPPUNIT_ASSERT(response.clientData() == FAKE_CLIENT_DATA);
    CPPUNIT_ASSERT(response.appStatusCode() == -128);
    CPPUNIT_ASSERT(response.appStatusString() == "");
    CPPUNIT_ASSERT(response.statusString() == "");
    CPPUNIT_ASSERT(response.results().size() == 1);
    CPPUNIT_ASSERT(response.clusterRoundTripTime() == FAKE_CLUSTER_ROUND_TRIP_TIME);
}

void testInvocationGeoInsertNulls() {
    SharedByteBuffer originalRequest  = fileAsByteBuffer("invocation_request_insert_geo_nulls.msg");
    SharedByteBuffer originalResponse = fileAsByteBuffer("invocation_response_insert_geo_nulls.msg");

    // These will be a null Geography and GeographyPoint.
    Geography      smallPoly;
    GeographyPoint smallPoint;
    std::vector<Parameter> params;
    params.push_back(Parameter(WIRE_TYPE_INTEGER, false));
    params.push_back(Parameter(WIRE_TYPE_GEOGRAPHY, false));
    params.push_back(Parameter(WIRE_TYPE_GEOGRAPHY_POINT, false));
    Procedure proc("InsertGeo", params);

    ParameterSet *ps = proc.params();
    ps->addInt32(201);
    ps->addGeography(smallPoly);
    ps->addGeographyPoint(smallPoint);
    int32_t procSize = proc.getSerializedSize();
    ScopedByteBuffer generatedRequest(procSize);
    proc.serializeTo(&generatedRequest, FAKE_CLIENT_DATA);

    // compare originalRequest and generatedRequest.
    compareRequestMessages(originalRequest,  "original_insert_geo_nulls.msg",
                           generatedRequest, "generated_insert_geo_nulls.msg");

    // Investigate the original response.  Again, this is less interesting.
    originalResponse.position(4);
    boost::shared_array<char> copy(new char[originalResponse.remaining()]);
    originalResponse.get(copy.get(), originalResponse.remaining());
    InvocationResponse response(copy, originalResponse.capacity() - 4);
    CPPUNIT_ASSERT(response.success());
    CPPUNIT_ASSERT(response.clientData() == FAKE_CLIENT_DATA);
    CPPUNIT_ASSERT(response.appStatusCode() == -128);
    CPPUNIT_ASSERT(response.appStatusString() == "");
    CPPUNIT_ASSERT(response.statusString() == "");
    CPPUNIT_ASSERT(response.results().size() == 1);
    CPPUNIT_ASSERT(response.clusterRoundTripTime() == FAKE_CLUSTER_ROUND_TRIP_TIME);
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
    compareByteBuffers(original, "original_all_types.msg",
                       buffer,   "generated_all_types.msg");
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
    CPPUNIT_ASSERT(columns[0].name() == "HELLO");
    CPPUNIT_ASSERT(columns[0].type() == WIRE_TYPE_STRING);
    CPPUNIT_ASSERT(columns[1].name() == "WORLD");
    CPPUNIT_ASSERT(columns[1].type() == WIRE_TYPE_STRING);
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
    CPPUNIT_ASSERT(columns[0].name() == "ID");
    CPPUNIT_ASSERT(columns[0].type() == WIRE_TYPE_BIGINT);
    CPPUNIT_ASSERT(columns[1].name() == "GEO");
    CPPUNIT_ASSERT(columns[1].type() == WIRE_TYPE_GEOGRAPHY);
    CPPUNIT_ASSERT(columns[2].name()== "GEO_PT");
    CPPUNIT_ASSERT(columns[2].type() == WIRE_TYPE_GEOGRAPHY_POINT);
    TableIterator iterator = results.iterator();
    int resultCount = 0;
    GeographyPoint originPoint(0, 0);
    Geography smallPoly;
    smallPoly.addEmptyRing() << GeographyPoint(0, 0)
                             << GeographyPoint(1, 0)
                             << GeographyPoint(1, 1)
                             << GeographyPoint(0, 1)
                             << GeographyPoint(0, 0);
    smallPoly.addEmptyRing()
                             << GeographyPoint(0.1, 0.1)
                             << GeographyPoint(0.1, 0.9)
                             << GeographyPoint(0.9, 0.9)
                             << GeographyPoint(0.9, 0.1)
                             << GeographyPoint(0.1, 0.1);
    while (iterator.hasNext()) {
        Row r = iterator.next();
        CPPUNIT_ASSERT(r.columnCount() == 3);
        CPPUNIT_ASSERT(r.getInt64("ID") == 100L);
        CPPUNIT_ASSERT(false == r.wasNull());
        CPPUNIT_ASSERT(r.getInt64(0) == 100L);
        CPPUNIT_ASSERT(false == r.wasNull());
        CPPUNIT_ASSERT(r.getGeography("GEO").approximatelyEqual(smallPoly, EPSILON));
        CPPUNIT_ASSERT(false == r.wasNull());
        CPPUNIT_ASSERT(r.getGeography(1).approximatelyEqual(smallPoly, EPSILON));
        CPPUNIT_ASSERT(false == r.wasNull());
        CPPUNIT_ASSERT(r.getGeographyPoint(2) == originPoint);
        CPPUNIT_ASSERT(false == r.wasNull());
        CPPUNIT_ASSERT(r.getGeographyPoint("GEO_PT") == originPoint);
        CPPUNIT_ASSERT(false == r.wasNull());
        resultCount++;
    }
    CPPUNIT_ASSERT(resultCount == 1);
}

void testInvocationGeoSelectBothMid() {
    SharedByteBuffer original = fileAsByteBuffer("invocation_response_select_geo_both_mid.msg");
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
    CPPUNIT_ASSERT(columns[0].name() == "ID");
    CPPUNIT_ASSERT(columns[0].type() == WIRE_TYPE_BIGINT);
    CPPUNIT_ASSERT(columns[1].name() == "GEO");
    CPPUNIT_ASSERT(columns[1].type() == WIRE_TYPE_GEOGRAPHY);
    CPPUNIT_ASSERT(columns[2].name() == "GEO_PT");
    CPPUNIT_ASSERT(columns[2].type() == WIRE_TYPE_GEOGRAPHY_POINT);
    TableIterator iterator = results.iterator();
    int resultCount = 0;
    GeographyPoint twentyPoint(20, 20);
    Geography midPoly;
    midPoly.addEmptyRing() << GeographyPoint( 0,  0)
                           << GeographyPoint(45,  0)
                           << GeographyPoint(45, 45)
                           << GeographyPoint( 0, 45)
                           << GeographyPoint( 0, 0);
    midPoly.addEmptyRing() << GeographyPoint(10, 10)
                           << GeographyPoint(10, 30)
                           << GeographyPoint(30, 30)
                           << GeographyPoint(30, 10)
                           << GeographyPoint(10, 10);
    while (iterator.hasNext()) {
        Row r = iterator.next();
        CPPUNIT_ASSERT(r.columnCount() == 3);
        CPPUNIT_ASSERT(r.getInt64("ID") == 101L);
        CPPUNIT_ASSERT(false == r.wasNull());
        CPPUNIT_ASSERT(r.getInt64(0) == 101L);
        CPPUNIT_ASSERT(false == r.wasNull());
        CPPUNIT_ASSERT(r.getGeography("GEO").approximatelyEqual(midPoly, EPSILON));
        CPPUNIT_ASSERT(false == r.wasNull());
        CPPUNIT_ASSERT(r.getGeography(1).approximatelyEqual(midPoly, EPSILON));
        CPPUNIT_ASSERT(false == r.wasNull());
        CPPUNIT_ASSERT(r.getGeographyPoint("GEO_PT") == twentyPoint);
        CPPUNIT_ASSERT(false == r.wasNull());
        CPPUNIT_ASSERT(r.getGeographyPoint(2) == twentyPoint);
        CPPUNIT_ASSERT(false == r.wasNull());
        resultCount++;
    }
    CPPUNIT_ASSERT(resultCount == 1);
}

void testInvocationGeoSelectPolyNull() {
    SharedByteBuffer original = fileAsByteBuffer("invocation_response_select_geo_polynull.msg");
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
    CPPUNIT_ASSERT(columns[0].name() == "ID");
    CPPUNIT_ASSERT(columns[0].type() == WIRE_TYPE_BIGINT);
    CPPUNIT_ASSERT(columns[1].name() == "GEO");
    CPPUNIT_ASSERT(columns[1].type() == WIRE_TYPE_GEOGRAPHY);
    CPPUNIT_ASSERT(columns[2].name() == "GEO_PT");
    CPPUNIT_ASSERT(columns[2].type() == WIRE_TYPE_GEOGRAPHY_POINT);
    TableIterator iterator = results.iterator();
    int resultCount = 0;
    GeographyPoint originPoint(0, 0);
    Geography smallPoly;
    smallPoly.addEmptyRing() << GeographyPoint(0, 0)
                             << GeographyPoint(1, 0)
                             << GeographyPoint(1, 1)
                             << GeographyPoint(0, 1)
                             << GeographyPoint(0, 0);
    while (iterator.hasNext()) {
        Row r = iterator.next();
        CPPUNIT_ASSERT(r.columnCount() == 3);
        CPPUNIT_ASSERT(r.getInt64("ID") == 102L);
        CPPUNIT_ASSERT(false == r.wasNull());
        CPPUNIT_ASSERT(r.getInt64(0) == 102L);
        CPPUNIT_ASSERT(false == r.wasNull());
        // The polygon is null here.  We will get an
        // answer, but it will be the null polygon.
        CPPUNIT_ASSERT(r.getGeography("GEO").isNull());
        CPPUNIT_ASSERT(true == r.wasNull());
        CPPUNIT_ASSERT(r.getGeography(1).isNull());
        CPPUNIT_ASSERT(true == r.wasNull());
        CPPUNIT_ASSERT(r.getGeographyPoint("GEO_PT") == originPoint);
        CPPUNIT_ASSERT(false == r.wasNull());
        CPPUNIT_ASSERT(r.getGeographyPoint(2) == originPoint);
        CPPUNIT_ASSERT(false == r.wasNull());
        resultCount++;
    }
    CPPUNIT_ASSERT(resultCount == 1);
}

void testInvocationGeoSelectPointNull() {
    SharedByteBuffer original = fileAsByteBuffer("invocation_response_select_geo_ptnull.msg");
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
    CPPUNIT_ASSERT(columns[0].name() == "ID");
    CPPUNIT_ASSERT(columns[0].type() == WIRE_TYPE_BIGINT);
    CPPUNIT_ASSERT(columns[1].name() == "GEO");
    CPPUNIT_ASSERT(columns[1].type() == WIRE_TYPE_GEOGRAPHY);
    CPPUNIT_ASSERT(columns[2].name() == "GEO_PT");
    CPPUNIT_ASSERT(columns[2].type() == WIRE_TYPE_GEOGRAPHY_POINT);
    TableIterator iterator = results.iterator();
    int resultCount = 0;
    Geography midPoly;
    midPoly.addEmptyRing() << GeographyPoint( 0,  0)
                           << GeographyPoint(45,  0)
                           << GeographyPoint(45, 45)
                           << GeographyPoint( 0, 45)
                           << GeographyPoint( 0,  0);
    midPoly.addEmptyRing() << GeographyPoint(10, 10)
                           << GeographyPoint(10, 30)
                           << GeographyPoint(30, 30)
                           << GeographyPoint(30, 10)
                           << GeographyPoint(10, 10);
    while (iterator.hasNext()) {
        Row r = iterator.next();
        CPPUNIT_ASSERT(r.columnCount() == 3);
        CPPUNIT_ASSERT(r.getInt64("ID") == 103L);
        CPPUNIT_ASSERT(false == r.wasNull());
        CPPUNIT_ASSERT(r.getInt64(0) == 103L);
        CPPUNIT_ASSERT(false == r.wasNull());
        Geography g103 = r.getGeography("GEO");
        CPPUNIT_ASSERT(true  == r.getGeography("GEO").approximatelyEqual(midPoly, EPSILON));
        CPPUNIT_ASSERT(false == r.wasNull());
        CPPUNIT_ASSERT(true  == r.getGeography(1).approximatelyEqual(midPoly, EPSILON));
        CPPUNIT_ASSERT(false == r.wasNull());
        CPPUNIT_ASSERT(r.getGeographyPoint("GEO_PT").isNull());
        CPPUNIT_ASSERT(true  == r.wasNull());
        CPPUNIT_ASSERT(r.getGeographyPoint(2).isNull());
        CPPUNIT_ASSERT(true  == r.wasNull());
        resultCount++;
    }
    CPPUNIT_ASSERT(resultCount == 1);
}

void testInvocationGeoSelectBothNull() {
    SharedByteBuffer original = fileAsByteBuffer("invocation_response_select_geo_bothnull.msg");
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
    CPPUNIT_ASSERT(columns[0].name() == "ID");
    CPPUNIT_ASSERT(columns[0].type() == WIRE_TYPE_BIGINT);
    CPPUNIT_ASSERT(columns[1].name() == "GEO");
    CPPUNIT_ASSERT(columns[1].type() == WIRE_TYPE_GEOGRAPHY);
    CPPUNIT_ASSERT(columns[2].name() == "GEO_PT");
    CPPUNIT_ASSERT(columns[2].type() == WIRE_TYPE_GEOGRAPHY_POINT);
    TableIterator iterator = results.iterator();
    int resultCount = 0;
    while (iterator.hasNext()) {
        Row r = iterator.next();
        CPPUNIT_ASSERT(r.columnCount() == 3);
        CPPUNIT_ASSERT(r.getInt64("ID") == 104L);
        CPPUNIT_ASSERT(false == r.wasNull());
        CPPUNIT_ASSERT(r.getInt64(0) == 104L);
        CPPUNIT_ASSERT(false == r.wasNull());
        CPPUNIT_ASSERT(true  == r.getGeography("GEO").isNull());
        CPPUNIT_ASSERT(true  == r.wasNull());
        CPPUNIT_ASSERT(true  == r.getGeography(1).isNull());
        CPPUNIT_ASSERT(true  == r.wasNull());
        CPPUNIT_ASSERT(true  == r.getGeographyPoint("GEO_PT").isNull());
        CPPUNIT_ASSERT(true  == r.wasNull());
        CPPUNIT_ASSERT(true  == r.getGeographyPoint(2).isNull());
        CPPUNIT_ASSERT(true  == r.wasNull());
        resultCount++;
    }
    CPPUNIT_ASSERT(resultCount == 1);
}

void testSerializedTable() {
    SharedByteBuffer original = fileAsByteBuffer("serialized_table.bin");
    original.position(4);
    Table t(original.slice());
    CPPUNIT_ASSERT(t.columnCount() == 9);
    CPPUNIT_ASSERT(t.rowCount() == 4);
    std::vector<Column> columns = t.columns();
    CPPUNIT_ASSERT(columns.size() == 9);
    CPPUNIT_ASSERT(columns[0].name() == "column1");
    CPPUNIT_ASSERT(columns[1].name() == "column2");
    CPPUNIT_ASSERT(columns[2].name() == "column3");
    CPPUNIT_ASSERT(columns[3].name() == "column4");
    CPPUNIT_ASSERT(columns[4].name() == "column5");
    CPPUNIT_ASSERT(columns[5].name() == "column6");
    CPPUNIT_ASSERT(columns[6].name() == "column7");
    CPPUNIT_ASSERT(columns[7].name() == "column8");
    CPPUNIT_ASSERT(columns[8].name() == "column9");
    CPPUNIT_ASSERT(columns[0].type() == WIRE_TYPE_TINYINT);
    CPPUNIT_ASSERT(columns[1].type() == WIRE_TYPE_STRING);
    CPPUNIT_ASSERT(columns[2].type() == WIRE_TYPE_SMALLINT);
    CPPUNIT_ASSERT(columns[3].type() == WIRE_TYPE_INTEGER);
    CPPUNIT_ASSERT(columns[4].type() == WIRE_TYPE_BIGINT);
    CPPUNIT_ASSERT(columns[5].type() == WIRE_TYPE_TIMESTAMP);
    CPPUNIT_ASSERT(columns[6].type() == WIRE_TYPE_DECIMAL);
    CPPUNIT_ASSERT(columns[7].type() == WIRE_TYPE_GEOGRAPHY);
    CPPUNIT_ASSERT(columns[8].type() == WIRE_TYPE_GEOGRAPHY_POINT);

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
    CPPUNIT_ASSERT(r.columnCount() == 9);

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
    CPPUNIT_ASSERT(r.columnCount() == 9);

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
    CPPUNIT_ASSERT(r.columnCount() == 9);

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

void testTableSerialization() {
    std::vector<Column> columns;
    generateTestSchema(columns);
    CPPUNIT_ASSERT(columns.size() == sizeof(schemaColTypes)/sizeof(schemaColTypes[0]));

    Table constructedTable (columns);
    RowBuilder row(columns);
    Geography smallPoly;
    Decimal dec;
    const std::string pi = "3.1459";

    smallPoly.addEmptyRing()
        << GeographyPoint(0, 0)
        << GeographyPoint(1, 0)
        << GeographyPoint(1, 1)
        << GeographyPoint(0, 1)
        << GeographyPoint(0, 0);
    smallPoly.addEmptyRing()
        << GeographyPoint(0.1, 0.1)
        << GeographyPoint(0.1, 0.9)
        << GeographyPoint(0.9, 0.9)
        << GeographyPoint(0.9, 0.1)
        << GeographyPoint(0.1, 0.1);
    GeographyPoint smallPoint(0.5, 0.5);

    //1st row - vt.addRow( null, null, null, null, null, null, null, poly, pt);
    for (int i = 0; i < sizeof(schemaColTypes)/sizeof(schemaColTypes[0]); ++i) {
        // all nulls except geo types
        if (schemaColTypes[i] == WIRE_TYPE_GEOGRAPHY) {
            row.addGeography(smallPoly);
        }
        else if (schemaColTypes[i] == WIRE_TYPE_GEOGRAPHY_POINT) {
            row.addGeographyPoint(smallPoint);
        } else {
            row.addNull();
        }
    }
    constructedTable.addRow(row);
    CPPUNIT_ASSERT(constructedTable.rowCount() == 1);

    // 2nd row - vt.addRow( 0, "", 2, 4, 5, new TimestampType(44), new BigDecimal("3.1459"), poly, pt);
    row.addInt8(0);
    row.addString("");
    row.addInt16(2);
    row.addInt32(4);
    row.addInt64(5);
    row.addTimeStamp(44);
    dec = Decimal(pi);
    row.addDecimal(dec);
    row.addGeography(smallPoly);
    row.addGeographyPoint(smallPoint);
    constructedTable.addRow(row);
    CPPUNIT_ASSERT(constructedTable.rowCount() == 2);

    // 3rd row - vt.addRow( 0, null, 2, 4, 5, null, null, poly, pt);
    row.addInt8(0);
    row.addNull();
    row.addInt16(2);
    row.addInt32(4);
    row.addInt64(5);
    row.addNull();
    row.addNull();
    row.addGeography(smallPoly);
    row.addGeographyPoint(smallPoint);
    constructedTable.addRow(row);
    CPPUNIT_ASSERT(constructedTable.rowCount() == 3);

    // 4th row - vt.addRow( null, "woobie", null, null, null, new TimestampType(44), new BigDecimal("3.1459"), poly, pt);
    row.addNull();
    row.addString("woobie");
    row.addNull();
    row.addNull();
    row.addNull();
    row.addTimeStamp(44);
    dec = Decimal(pi);
    row.addDecimal(dec);
    row.addGeography(smallPoly);
    row.addGeographyPoint(smallPoint);
    constructedTable.addRow(row);
    CPPUNIT_ASSERT(constructedTable.rowCount() == 4);

    int32_t serializedTableSize = constructedTable.getSerializedSize();
    char *constructedData = (char *) malloc(serializedTableSize);
    SharedByteBuffer generated(constructedData, serializedTableSize);
    int32_t serializedSize = constructedTable.serializeTo(generated);
    CPPUNIT_ASSERT(serializedSize == serializedTableSize);
    int32_t tableDataSizeFromBB = generated.getInt32(0);
    CPPUNIT_ASSERT((serializedSize - 4) == tableDataSizeFromBB);
    SharedByteBuffer original = fileAsByteBuffer("serialized_table.bin");

    CPPUNIT_ASSERT(tableDataSizeFromBB == generated.getInt32(0));

    original.position(4);
    Table tableFromSBB(original.slice());
    CPPUNIT_ASSERT(constructedTable.columnCount() == tableFromSBB.columnCount());
    CPPUNIT_ASSERT(constructedTable.columns() == tableFromSBB.columns());
    CPPUNIT_ASSERT(constructedTable.rowCount() == tableFromSBB.rowCount());

    compareTables(constructedTable, tableFromSBB);
}

void compareTables(Table &t1, Table &t2) {
    int32_t scanRow = 0;
    const int32_t rowCount = t1.rowCount();

    TableIterator t1Itr = t1.iterator();
    TableIterator t2Itr = t2.iterator();

    int8_t t1col1Val, t2col1Val;
    std::string t1col2Val, t2col2Val;
    int16_t t1col3Val, t2col3Val;
    int32_t t1col4Val, t2col4Val;
    int64_t t1col5Val, t2col5Val;
    int64_t t1col6Val, t2col6Val;
    Decimal t1col7Val, t2col7Val;
    Geography t1col8Val, t2col8Val;
    GeographyPoint t1col9Val, t2col9Val;

    while (scanRow < rowCount) {
        CPPUNIT_ASSERT(t1Itr.hasNext() == t2Itr.hasNext());
        Row row1 = t1Itr.next();
        Row row2 = t2Itr.next();
        CPPUNIT_ASSERT(row1.columnCount() == row2.columnCount());
        CPPUNIT_ASSERT(row1.columns() == row2.columns());

        CPPUNIT_ASSERT(row1.isNull(0) == row2.isNull(0));
        if (!row1.isNull(0)) {
            //std::cout << (int32_t) row1.getInt8(0) << " " << (int32_t) row2.getInt8(0) << ", ";
            CPPUNIT_ASSERT(row1.getInt8(0) == row2.getInt8(0));
        }

        CPPUNIT_ASSERT(row1.isNull(1) == row2.isNull(1));
        if (!row1.isNull(1)) {
            //std::cout << row1.getString(1) << " " << row2.getString(1) << ", ";
            CPPUNIT_ASSERT(row1.getString(1) == row2.getString(1));
        }

        CPPUNIT_ASSERT(row1.isNull(2) == row2.isNull(2));
        if (!row1.isNull(2)) {
            //std::cout << (int32_t) row1.getInt16(2) << " " << (int32_t) row2.getInt16(2) << ", ";
            CPPUNIT_ASSERT(row1.getInt16(2) == row2.getInt16(2));
        }

        CPPUNIT_ASSERT(row1.isNull(3) == row2.isNull(3));
        if (!row1.isNull(3)) {
            //std::cout << row1.getInt32(3) << " " << row2.getInt32(3) << ", ";
            CPPUNIT_ASSERT(row1.getInt32(3) == row2.getInt32(3));
        }

        CPPUNIT_ASSERT(row1.isNull(4) == row2.isNull(4));
        if (!row1.isNull(4)) {
            //std::cout << row1.getInt64(4) << " " << row2.getInt64(4) << ", ";
            CPPUNIT_ASSERT(row1.getInt64(4) == row2.getInt64(4));
        }

        CPPUNIT_ASSERT(row1.isNull(5) == row2.isNull(5));
        if (!row1.isNull(5)) {
            //std::cout << row1.getTimestamp(5) << " " << row2.getTimestamp(5) << ", ";
            CPPUNIT_ASSERT(row1.getTimestamp(5) == row2.getTimestamp(5));
        }

        CPPUNIT_ASSERT(row1.isNull(6) == row2.isNull(6));
        if (!row1.isNull(6)) {
            //std::cout << row1.getDecimal(6).toString() << " " << row2.getDecimal(6).toString() << ", ";
            CPPUNIT_ASSERT(row1.getDecimal(6) == row2.getDecimal(6));
        }

        CPPUNIT_ASSERT(row1.isNull(7) == row2.isNull(7));
        if (!row1.isNull(7)) {
            //std::cout << row1.getGeography(7).toString() << " " << row2.getGeography(7).toString() <<", ";
            CPPUNIT_ASSERT(row1.getGeography(7) == row2.getGeography(7));
        }

        CPPUNIT_ASSERT(row1.isNull(8) == row2.isNull(8));
        if (!row1.isNull(8)) {
            //std::cout << row1.getGeographyPoint(8).toString() << " " << row2.getGeographyPoint(8).toString();
            CPPUNIT_ASSERT(row1.getGeographyPoint(8)== row2.getGeographyPoint(8));
        }

        ++scanRow;
    }
}

};

const std::string SerializationTest::FAKE_BUILD_STRING("volt_6.1_test_build_string");

CPPUNIT_TEST_SUITE_REGISTRATION( SerializationTest );
}
