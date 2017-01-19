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
#include "Table.h"
#include "TableIterator.h"
#include "RowBuilder.h"
#include "Decimal.hpp"
#include <boost/scoped_ptr.hpp>

namespace voltdb {

voltdb::WireType allTypes[11] = {
            WIRE_TYPE_TINYINT,
            WIRE_TYPE_SMALLINT,
            WIRE_TYPE_INTEGER,
            WIRE_TYPE_BIGINT,
            WIRE_TYPE_FLOAT,
            WIRE_TYPE_STRING,
            WIRE_TYPE_TIMESTAMP,
            WIRE_TYPE_DECIMAL,
            WIRE_TYPE_VARBINARY,
            WIRE_TYPE_GEOGRAPHY_POINT,
            WIRE_TYPE_GEOGRAPHY,
};


class TableTest : public CppUnit::TestFixture {
private:
    CPPUNIT_TEST_SUITE( TableTest );
    CPPUNIT_TEST(testTableConstruct);
    CPPUNIT_TEST(testRowTableConstructWithEmptySchemaNegative);
    CPPUNIT_TEST(testRowBuilderRudimentary);
    CPPUNIT_TEST(testTablePopulate);
    CPPUNIT_TEST(testPopulateRowWithIllegalColumnType);
    CPPUNIT_TEST(testDecimalSignedZeroEquality);
    CPPUNIT_TEST(testTableSerializeNegative);
    CPPUNIT_TEST_SUITE_END();

    void fillRandomValues() {
        m_tinyIntValue = rand() % INT8_MAX;
        m_shortValue = rand() % INT16_MAX;
        m_integerValue = rand() % INT32_MAX;
        m_longValue = INT64_MAX - rand();
        m_floatValue = rand() / 17;
        m_stringValue = "V01tta8l3Col";
        m_timestampValue = rand() / 37;
        TTInt ttInt;
        ttInt.SetMin();
        ttInt +=  rand() + 1;
        m_decValue = Decimal(ttInt);

        for (int i = 0; i < sizeof (m_binData)/sizeof(m_binData[0]); ++i) {
            m_binData[i] = rand() % INT8_MAX;
        }

        m_ptValue = GeographyPoint(30.33, 60.66);
        m_polyValue.addEmptyRing()
                << GeographyPoint(0, 0)
                << GeographyPoint(15.3333, 0)
                << GeographyPoint(15.999, 15.666)
                << GeographyPoint(0, 14.1)
                << GeographyPoint(0, 0);;
    }

    void someMaxValues() {
        m_tinyIntValue = INT8_MAX;
        m_shortValue = INT16_MAX;
        m_integerValue = INT32_MAX;
        m_longValue = INT64_MAX;
        m_floatValue = rand() / 17;
        m_stringValue = "V01tta8l3Col123456789foobar";
        m_timestampValue = INT64_MAX;
        TTInt ttInt;
        ttInt.SetMax();
        m_decValue = Decimal(ttInt);
        for (int i = 0; i < sizeof (m_binData)/sizeof(m_binData[0]); ++i) {
                m_binData[i] = INT8_MAX;
        }
        m_ptValue = GeographyPoint(30.33, 60.66);
        m_polyValue.addEmptyRing()
                << GeographyPoint(0, 0)
                << GeographyPoint(5.3333, 0.444412432)
                << GeographyPoint(15.3333, 0)
                << GeographyPoint(15.999, 15.666)
                << GeographyPoint(15.99999999911, 36.333333333)
                << GeographyPoint(13.99999999911, 36.333333333)
                << GeographyPoint(0, 14.12345)
                << GeographyPoint(0, 0);;
    }

    // test constants
    int8_t m_tinyIntValue;
    int16_t m_shortValue;
    int32_t m_integerValue;
    int64_t m_longValue;
    double m_floatValue;
    std::string m_stringValue;
    int64_t m_timestampValue;
    Decimal m_decValue;
    uint8_t m_binData[1024];
    GeographyPoint m_ptValue;
    Geography m_polyValue;

    int32_t populateRow(RowBuilder &row, bool nullValue) {
        int32_t rowSize = 0;
        for (int i = 0; i < sizeof(allTypes)/sizeof(allTypes[0]); i++) {
            rowSize += populateValue(row, allTypes[i], nullValue);
        }
        return rowSize;
    }
    int32_t populateValue(RowBuilder &row, WireType type, bool nullValue) {
        if (nullValue) {
            int32_t length = 0;
            switch (type) {
            case WIRE_TYPE_TINYINT:
                length = 1;
                break;
            case WIRE_TYPE_SMALLINT:
                length = 2;
                break;
            case WIRE_TYPE_INTEGER:
                length = 4;
                break;
            case WIRE_TYPE_BIGINT:
                length = 8;
                break;
            case WIRE_TYPE_FLOAT:
                length = 8;
                break;
            case WIRE_TYPE_STRING:
                length = 4;
                break;
            case WIRE_TYPE_TIMESTAMP:
                length = 8;
                break;
            case WIRE_TYPE_DECIMAL:
                length = 2*sizeof(int64_t);
                break;
            case WIRE_TYPE_VARBINARY:
                length = 4;
                break;
            case WIRE_TYPE_GEOGRAPHY_POINT:
                length =  2 * sizeof(double);
                break;
            case WIRE_TYPE_GEOGRAPHY:
                length = 4;
                break;
            default:
                std::cout<< (int) type << " " << wireTypeToString(type);
                throw InvalidColumnException();
            }
            row.addNull();
            return length;
        }

        // non null
        switch (type) {
        case WIRE_TYPE_TINYINT:
            row.addInt8(m_tinyIntValue);
            return 1;
        case WIRE_TYPE_SMALLINT:
            row.addInt16(m_shortValue);
            return 2;
        case WIRE_TYPE_INTEGER:
            row.addInt32(m_integerValue);
            return 4;
        case WIRE_TYPE_BIGINT:
            row.addInt64(m_longValue);
            return 8;
        case WIRE_TYPE_FLOAT:
            row.addDouble(m_floatValue);
            return 8;
        case WIRE_TYPE_STRING: {
            row.addString(m_stringValue);
            return 4 + m_stringValue.size();
        }
        case WIRE_TYPE_TIMESTAMP:
            row.addTimeStamp(m_timestampValue);
            return 8;
        case WIRE_TYPE_DECIMAL: {
            row.addDecimal(m_decValue);
            return 2*sizeof(int64_t);
        }
        case WIRE_TYPE_VARBINARY: {
            row.addVarbinary(sizeof(m_binData), m_binData);
            return 4 + sizeof(m_binData);
        }
        case WIRE_TYPE_GEOGRAPHY_POINT: {
            row.addGeographyPoint(m_ptValue);
            return (2 * sizeof(double));
        }
        case WIRE_TYPE_GEOGRAPHY: {
            row.addGeography(m_polyValue);
            return m_polyValue.getSerializedSize();
        }
        default:
            std::cout<< (int) type << " " << wireTypeToString(type);
            throw InvalidColumnException();
        }
        return -1;      // make compiler happy
    }

    void verifyRowData(Row& row) {
        int32_t colCount = row.columnCount();
        std::vector<Column> types = row.columns();

        for (int32_t i = 0; i < colCount; ++i) {
            switch (types[i].type()) {
            case WIRE_TYPE_TINYINT: {
                int8_t value = row.getInt8(i);
                CPPUNIT_ASSERT(value == m_tinyIntValue);
                break;
            }
            case WIRE_TYPE_SMALLINT: {
                int16_t value = row.getInt16(i);
                CPPUNIT_ASSERT(value == m_shortValue);
                break;
            }
            case WIRE_TYPE_INTEGER: {
                int32_t value = row.getInt32(i);
                CPPUNIT_ASSERT(value == m_integerValue);
                break;
            }
            case WIRE_TYPE_BIGINT: {
                long value = row.getInt64(i);
                CPPUNIT_ASSERT(value == m_longValue);
                break;
            }
            case WIRE_TYPE_FLOAT: {
                double value = row.getDouble(i);
                CPPUNIT_ASSERT(value == m_floatValue);
                break;
            }
            case WIRE_TYPE_STRING: {
                std::string value = row.getString(i);
                CPPUNIT_ASSERT(value == m_stringValue);
                break;
            }
            case WIRE_TYPE_TIMESTAMP: {
                int64_t value = row.getTimestamp(i);
                CPPUNIT_ASSERT(value == m_timestampValue);
                break;
            }
            case WIRE_TYPE_DECIMAL: {
                Decimal value = row.getDecimal(i);
                CPPUNIT_ASSERT(value == m_decValue);
                break;
            }
            case WIRE_TYPE_VARBINARY: {
                uint8_t* data = (uint8_t*) malloc(sizeof(m_binData));
                boost::scoped_ptr<uint8_t> value(data);
                int outLen = 0;
                bool status = row.getVarbinary(i, sizeof(m_binData), data, &outLen);
                CPPUNIT_ASSERT(status);
                CPPUNIT_ASSERT(outLen == sizeof(m_binData));
                CPPUNIT_ASSERT((::memcmp((void *) m_binData, (void *) data, outLen) == 0));
                break;
            }
            case WIRE_TYPE_GEOGRAPHY_POINT: {
                GeographyPoint value = row.getGeographyPoint(i);
                CPPUNIT_ASSERT(value == m_ptValue);
                break;
            }
            case WIRE_TYPE_GEOGRAPHY: {
                Geography value = row.getGeography(i);
                CPPUNIT_ASSERT(value == m_polyValue);
                break;
            }
            default:
                std::cout<< i  << " " << wireTypeToString(types[i].type());
                throw InvalidColumnException();
            }
        }
    }


    void verifyNullValues(Row& row) {
        int32_t colCount = row.columnCount();
        std::vector<Column> types = row.columns();

        for (int32_t i = 0; i < colCount; ++i) {
            switch (types[i].type()) {
            case WIRE_TYPE_TINYINT: {
                row.getInt8(i);
                break;
            }

            case WIRE_TYPE_SMALLINT: {
                row.getInt16(i);
                break;
            }

            case WIRE_TYPE_INTEGER: {
                row.getInt32(i);
                break;
            }

            case WIRE_TYPE_BIGINT: {
                row.getInt64(i);
                break;
            }

            case WIRE_TYPE_FLOAT: {
                row.getDouble(i);
                break;
            }

            case WIRE_TYPE_STRING: {
                row.getString(i);
                break;
            }

            case WIRE_TYPE_TIMESTAMP: {
                row.getTimestamp(i);
                break;
            }

            case WIRE_TYPE_DECIMAL: {
                row.getDecimal(i);
                break;
            }

            case WIRE_TYPE_VARBINARY: {
                uint8_t* data = (uint8_t*) malloc(sizeof(m_binData));
                boost::scoped_ptr<uint8_t> value(data);
                int outLen = 0;
                bool status = row.getVarbinary(i, sizeof(m_binData), data, &outLen);
                CPPUNIT_ASSERT(status);
                break;
            }

            case WIRE_TYPE_GEOGRAPHY_POINT: {
                row.getGeographyPoint(i);
                break;
            }

            case WIRE_TYPE_GEOGRAPHY: {
                row.getGeography(i);
                break;
            }

            default:
                std::cout<< i  << " " << wireTypeToString(types[i].type());
                throw InvalidColumnException();
            }
            CPPUNIT_ASSERT(row.wasNull());
        }
    }
    public:

        void testTableConstruct() {
            std::vector<voltdb::Column> columns;
            columns.push_back(voltdb::Column("foo", voltdb::WIRE_TYPE_BIGINT));
            columns.push_back(voltdb::Column("foo1", voltdb::WIRE_TYPE_TINYINT));
            Table table (columns);

            CPPUNIT_ASSERT(columns == table.columns());
            CPPUNIT_ASSERT(table.columnCount() == (int32_t) columns.size());

            // validate the constituted buffer
            CPPUNIT_ASSERT(table.m_buffer.getInt8(4) == Table::DEFAULT_STATUS_CODE);
            CPPUNIT_ASSERT(table.m_buffer.getInt16(5) == (int16_t) columns.size());
            int index = 0;
            for (index = 0; index < columns.size(); ++index) {
                CPPUNIT_ASSERT(table.m_buffer.getInt8(7 + index) == columns[index].type());
            }

            int32_t rowCountPosition = table.m_rowCountPosition;
            CPPUNIT_ASSERT(table.m_buffer.getInt32(0) == (rowCountPosition - 4));
            CPPUNIT_ASSERT(table.m_buffer.getInt32(rowCountPosition) == 0);
        }

        void allTypeColumns(std::vector<voltdb::Column>& columns) {
            if (!columns.empty()) {
                columns.clear();
            }
            for(int i = 0; i < sizeof(allTypes)/sizeof(allTypes[0]); ++i) {
                columns.push_back(voltdb::Column("Col"+ wireTypeToString(allTypes[i]), allTypes[i]));
            }
        }

        void testTablePopulate() {
            std::vector<voltdb::Column> columns;
            allTypeColumns(columns);

            Table table (columns);
            RowBuilder row(columns);

            fillRandomValues();
            populateRow(row, false);
            table.addRow(row);
            CPPUNIT_ASSERT(table.rowCount() == 1);

            // verify first row - it has random data
            TableIterator tableItr = table.iterator();
            assert (tableItr.hasNext());
            Row rowFetched = tableItr.next();
            CPPUNIT_ASSERT(rowFetched.columnCount() == columns.size());
            std::vector<Column> schemaRead = rowFetched.columns();
            CPPUNIT_ASSERT(schemaRead == columns);
            verifyRowData(rowFetched);

            // second row some max values
            someMaxValues();
            populateRow(row, false);
            table.addRow(row);
            CPPUNIT_ASSERT(table.rowCount() == 2);

            // third row null values
            populateRow(row, true);
            table.addRow(row);
            CPPUNIT_ASSERT(table.rowCount() == 3);

            tableItr = table.iterator();
            assert (tableItr.hasNext());
            rowFetched = tableItr.next(); // 1st row
            CPPUNIT_ASSERT(rowFetched.columnCount() == columns.size());
            schemaRead = rowFetched.columns();
            CPPUNIT_ASSERT(schemaRead == columns);

            assert(tableItr.hasNext());     // 2nd row
            rowFetched = tableItr.next();
            CPPUNIT_ASSERT(rowFetched.columnCount() == columns.size());
            schemaRead = rowFetched.columns();
            CPPUNIT_ASSERT(schemaRead == columns);
            verifyRowData(rowFetched);

            assert (tableItr.hasNext());    // 3rd row
            rowFetched = tableItr.next();
            CPPUNIT_ASSERT(rowFetched.columnCount() == columns.size());
            schemaRead = rowFetched.columns();
            CPPUNIT_ASSERT(schemaRead == columns);
            verifyNullValues(rowFetched);

            assert (!tableItr.hasNext());   // no more rows left

            // serialization
            int32_t tableSerializeSize = table.getSerializedSize();
            char *data = new char[tableSerializeSize];
            boost::scoped_ptr<char> scopedData(data);
            ByteBuffer tableBB(scopedData.get(), tableSerializeSize);
            int32_t serializedSize = table.serializeTo(tableBB);
            if (tableSerializeSize != serializedSize) {
                std::cout << "expected serialized size: " << tableSerializeSize <<" got: " << serializedSize <<std::endl;
            }
            CPPUNIT_ASSERT(serializedSize == tableSerializeSize);

            // verify the table size from serialized bb
            int32_t tableSize = tableBB.getInt32(0);
            CPPUNIT_ASSERT((serializedSize - 4) == tableSize);
        }

        void testTableSerializeNegative() {
            // serialize the table with partially populated table
            std::vector<voltdb::Column> columns;
            columns.push_back(voltdb::Column("foo", voltdb::WIRE_TYPE_BIGINT));
            columns.push_back(voltdb::Column("foo1", voltdb::WIRE_TYPE_TINYINT));
            Table table (columns);

            RowBuilder row(columns);
            row.addInt64(INT64_MAX -1);
            int32_t dataSize = row.getSerializedSize();
            char *data = (char *) malloc(dataSize);
            SharedByteBuffer buffer(data, dataSize);
            try {
                row.serializeTo(buffer);
                CPPUNIT_ASSERT_MESSAGE("serialization of partially populated expected to fail", false);
            }
            catch (const voltdb::UninitializedColumnException &excp) {
                std::string msg(excp.what());
                std::string expected = "Row must contain data for all columns. 2 columns required, only 1 columns provided";
                CPPUNIT_ASSERT_MESSAGE("Exception message missing supplied type name",
                                        msg.find(expected) != std::string::npos);
            }
        }

        void testRowBuilderRudimentary() {
            std::vector<voltdb::Column> columns;
            allTypeColumns(columns);

            fillRandomValues();
            RowBuilder row(columns);
            int32_t rowDataSize = populateRow(row, false);

            int32_t serializedSize = row.getSerializedSize();
            if ((rowDataSize + 4) != serializedSize) {
                std::cout << __LINE__ << (rowDataSize+4) << " : " << serializedSize << std::endl;
            }
            CPPUNIT_ASSERT((rowDataSize + 4) == serializedSize);

            char* data = (char *) malloc(serializedSize);
            SharedByteBuffer byteBuffer(data, serializedSize);

            byteBuffer.ensureRemaining(serializedSize);
            rowDataSize = row.serializeTo(byteBuffer);
            CPPUNIT_ASSERT(rowDataSize == serializedSize);

            // populate with nulls
            rowDataSize = populateRow(row, true);
            serializedSize = row.getSerializedSize();
            if ((rowDataSize + 4) != serializedSize) {
                std::cout << __LINE__ << (rowDataSize+4) << " : " << serializedSize << std::endl;
            }
            CPPUNIT_ASSERT((rowDataSize + 4) == serializedSize);

            byteBuffer.clear();
            byteBuffer.ensureRemaining(serializedSize);
            rowDataSize = row.serializeTo(byteBuffer);
            CPPUNIT_ASSERT(rowDataSize == serializedSize);

            // verify the row size from serialized bb
            int32_t rowSize = byteBuffer.getInt32(0);
            CPPUNIT_ASSERT((serializedSize - 4) == rowSize);
        }

        void testRowTableConstructWithEmptySchemaNegative() {
            // constructing row or column with no column/empty
            // schema not permissible
            std::vector<voltdb::Column> columns;
            std::string expected;

            try {
                Table table (columns);
                CPPUNIT_ASSERT_MESSAGE("table construction with empty schema expected to fail", false);
            }
            catch (const voltdb::TableException &excp) {
                std::string msg(excp.what());
                expected = "Failed to create table. Provided schema can't be empty, it must contain at least one column";
                CPPUNIT_ASSERT_MESSAGE("Exception message empty schema message not found",
                                        msg.find(expected) != std::string::npos);
            }

            try {
                RowBuilder row(columns);
                CPPUNIT_ASSERT_MESSAGE("row construction with empty schema expected to fail", false);
            }
            catch (const voltdb::RowCreationException &excp) {
                std::string msg(excp.what());
                expected = "Failed to create row. The schema for row must contain at least one column";
                CPPUNIT_ASSERT_MESSAGE("Exception message empty schema message not found",
                                       msg.find(expected) != std::string::npos);
            }
        }

        void testPopulateRowWithIllegalColumnType () {
            std::vector<voltdb::Column> columns(3);
            columns[0] = voltdb::Column("C1", voltdb::WIRE_TYPE_BIGINT);
            columns[1] = voltdb::Column("C2", voltdb::WIRE_TYPE_BIGINT);
            columns[2] = voltdb::Column("C3", voltdb::WIRE_TYPE_BIGINT);

            RowBuilder row(columns);
            row.addInt64(10);
            row.addInt64(4);
            try {
                row.addString("foo");
                CPPUNIT_ASSERT_MESSAGE("Expected to throw InvalidColumnException", false);
            }
            catch (voltdb::InvalidColumnException& excp) {
                std::string msg(excp.what());
                CPPUNIT_ASSERT_MESSAGE("Exception message missing expected type name",
                        msg.find(voltdb::wireTypeToString(voltdb::WIRE_TYPE_BIGINT)) != std::string::npos);
                CPPUNIT_ASSERT_MESSAGE("Exception message missing supplied type name",
                        msg.find(voltdb::wireTypeToString(voltdb::WIRE_TYPE_STRING)) != std::string::npos);
            }
        }

        // type specific test for testing signed zero equality
        void testDecimalSignedZeroEquality() {
            TTInt positiveZero;
            positiveZero.SetZero();
            TTInt negativeZero;
            negativeZero.SetZero();
            negativeZero.SetSign();
            CPPUNIT_ASSERT(positiveZero == negativeZero);

            Decimal d1 (positiveZero);
            Decimal d2 (negativeZero);
            CPPUNIT_ASSERT(d1 == d2);

            std::string value = "0.0";
            d1 = Decimal(value);
            value = "-0.0";
            d2 = Decimal(value);
            CPPUNIT_ASSERT(d1 == d2);

            value = "0.00000"; d1 = Decimal(value);
            value = "-0.000"; d2 = Decimal(value);
            CPPUNIT_ASSERT(d1 == d2);
        }
    };

    CPPUNIT_TEST_SUITE_REGISTRATION( TableTest );
}
