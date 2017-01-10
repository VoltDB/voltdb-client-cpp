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
CPPUNIT_TEST_SUITE( TableTest );
CPPUNIT_TEST(testTableConstruct);
CPPUNIT_TEST_EXCEPTION(testTableConstructWitnNoColumns, voltdb::VoltTableException);
CPPUNIT_TEST(testRowBuilderRudimentary);
CPPUNIT_TEST(testRowAdd);
CPPUNIT_TEST_SUITE_END();




int32_t populateAllTypeSomeValue(RowBuilder &row) {
    //std::cout << std::endl << sizeof(allTypes)/sizeof(allTypes[0]) << std::endl;
    int32_t rowSize = 0;
    for (int i = 0; i < sizeof(allTypes)/sizeof(allTypes[0]); i++) {
        //std::cout << i << " type: " << wireTypeToString(allTypes[i]) << " position: " << row.m_buffer.position() << std::endl;
        rowSize += populateSomeValue(row, allTypes[i]);
        //std::cout << "update rowSize: " << rowSize << std::endl;
    }
    return rowSize;
}
int32_t populateSomeValue(RowBuilder &row, WireType type) {
    switch (type) {
    case WIRE_TYPE_TINYINT:
        row.addInt8(INT8_MIN);
        return 1;
    case WIRE_TYPE_SMALLINT:
        row.addInt16(INT16_MIN);
        return 2;
    case WIRE_TYPE_INTEGER:
        row.addInt32(INT32_MIN);
        return 4;
    case WIRE_TYPE_BIGINT:
        row.addInt64(INT64_MIN);
        return 8;
    case WIRE_TYPE_FLOAT:
        row.addDouble(0.00001);
        return 8;
    case WIRE_TYPE_STRING: {
        std::string value = "foo1";
        row.addString("foo1");
        return 4 + value.size();
    }
    case WIRE_TYPE_TIMESTAMP:
        row.addTimeStamp(6.77777);
        return 8;
    case WIRE_TYPE_DECIMAL: {
        TTInt value;
        value.SetMin();
        value = value + 1;
        Decimal d = Decimal(value);
        row.addDecimal(d);
        return 2*sizeof(int64_t);
    }
    case WIRE_TYPE_VARBINARY: {
        uint8_t data[8] = {0, 2, 3, 5, 8, 3, 4, 7};
        row.addVarbinary(sizeof(data), data);
        return 4 + sizeof(data);
    }
    case WIRE_TYPE_GEOGRAPHY_POINT: {
        GeographyPoint pt = GeographyPoint(30, 30);
        row.addGeographyPoint(pt);
        return (2 * sizeof(double));
    }
    case WIRE_TYPE_GEOGRAPHY: {
        Geography poly;
        poly.addEmptyRing()
                    << GeographyPoint(0, 0)
                    << GeographyPoint(1, 0)
                    << GeographyPoint(1, 1)
                    << GeographyPoint(0, 1)
                    << GeographyPoint(0, 0);

        row.addGeography(poly);
        return poly.getSerializedSize();
    }
    default:
        std::cout<< (int) type << " " << wireTypeToString(type);
        throw InvalidColumnException();
    }
    return -1;      // make compiler happy
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
        CPPUNIT_ASSERT(table.m_buffer.getInt8(4) == 0);
        CPPUNIT_ASSERT(table.m_buffer.getInt16(5) == (int16_t) columns.size());
        int index = 0;
        for (index = 0; index < columns.size(); ++index) {
            CPPUNIT_ASSERT(table.m_buffer.getInt8(7 + index) == columns[index].type());
        }

        int32_t rowCountPosition = table.m_rowStart;
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
    void testRowAdd() {
        std::vector<voltdb::Column> columns;
        allTypeColumns(columns);

        Table table (columns);
        RowBuilder row(columns);
        int32_t rowSize = populateAllTypeSomeValue(row);
        table.addRow(row);
        CPPUNIT_ASSERT(table.rowCount() == 1);
    }

    void testRowBuilderRudimentary() {
        std::vector<voltdb::Column> columns;
        allTypeColumns(columns);

        RowBuilder row(columns);
        int32_t rowSize = populateAllTypeSomeValue(row);

        char* data = (char *) malloc(8192);
        SharedByteBuffer byteBuffer(data, 8192);
        //byteBuffer.ensureRemaining(8192);
        std::cout << std::endl << "position: " << byteBuffer.position() << "; limit: " << byteBuffer.limit() << std::endl;
        row.serializeTo(byteBuffer);
        std::cout << "position: " << byteBuffer.position() << "; limit: " << byteBuffer.limit() << " row size: " << rowSize << std::endl;
        CPPUNIT_ASSERT(rowSize== byteBuffer.position());
    }

    void testTableConstructWitnNoColumns() {
        std::vector<voltdb::Column> columns;
        Table table (columns);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( TableTest );
}
