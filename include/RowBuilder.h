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

#ifndef VOLTDB_ROWBUILDER_H_
#define VOLTDB_ROWBUILDER_H_
#include "ByteBuffer.hpp"
#include "WireType.h"
#include "Column.hpp"
#include "boost/shared_ptr.hpp"
#include <stdint.h>
#include "Exception.hpp"
#include "Decimal.hpp"
#include "Geography.hpp"
#include "GeographyPoint.hpp"

/* Helper class to build row given the table schema.
 * Column count and it's type is inferred using the table schema
 * provided at construction time.
 * Note: the bytebuffer does not contain the Row length field at the start
 * ByteBuffer stores the column data sequentially starting with first column
 */

namespace voltdb {

class TableTest;
class RowBuilder {
friend class TableTest;
private:
    void validateType(WireType type) throw (InvalidColumnException, ColumnPopulateException){
        if (m_currentColumnIndex >= m_columns.size()) {
            throw InvalidColumnException(m_currentColumnIndex, m_columns.size());
        }

        if (m_columns[m_currentColumnIndex].type() != type) {
            throw ColumnPopulateException(m_columns[m_currentColumnIndex].type(), type);
        }
    }

public:
    // Initializes the Rowbuilder with schema of the table for which the row will be constructed
    RowBuilder(const std::vector<Column> &schema) throw (ColumnPopulateException);

    RowBuilder& addInt64(int64_t val) {
        validateType(WIRE_TYPE_BIGINT);
        m_buffer.ensureRemaining(8);
        m_buffer.putInt64(val);
        m_currentColumnIndex++;
        return *this;
    }

    RowBuilder& addInt32(int32_t val) {
        validateType(WIRE_TYPE_INTEGER);
        m_buffer.ensureRemaining(4);
        m_buffer.putInt32(val);
        m_currentColumnIndex++;
        return *this;
    }

    RowBuilder& addInt16(int16_t val) {
        validateType(WIRE_TYPE_SMALLINT);
        m_buffer.ensureRemaining(2);
        m_buffer.putInt16(val);
        m_currentColumnIndex++;
        return *this;
    }

    RowBuilder& addInt8(int8_t val) {
        validateType(WIRE_TYPE_TINYINT);
        m_buffer.ensureRemaining(1);
        m_buffer.putInt8(val);
        m_currentColumnIndex++;
        return *this;
    }

    RowBuilder& addDouble(double val) {
        validateType(WIRE_TYPE_FLOAT);
        m_buffer.ensureRemaining(8);
        m_buffer.putDouble(val);
        m_currentColumnIndex++;
        return *this;
    }

    RowBuilder& addNull() {
        if (m_currentColumnIndex >= m_columns.size()) {
            throw InvalidColumnException(m_currentColumnIndex, m_columns.size());
        }

        switch (m_columns[m_currentColumnIndex].type()) {
        case WIRE_TYPE_BIGINT:
        case WIRE_TYPE_TIMESTAMP:
            addInt64(INT64_MIN);
            break;
        case WIRE_TYPE_INTEGER:
            addInt32(INT32_MIN);
            break;
        case WIRE_TYPE_SMALLINT:
            addInt16(INT16_MIN);
            break;
        case WIRE_TYPE_TINYINT:
            addInt8(INT8_MIN);
            break;
        case WIRE_TYPE_FLOAT:
            addDouble(-1.7976931348623157E+308);
            break;
        case WIRE_TYPE_STRING:
            m_buffer.ensureRemaining(4);
            m_buffer.putInt32(-1);
            m_currentColumnIndex++;
            break;
        case WIRE_TYPE_VARBINARY:
            m_buffer.ensureRemaining(4);
            m_buffer.putInt32(-1);
            m_currentColumnIndex++;
            break;
         //todo: populate null for decimal, geography point and geography types
        case WIRE_TYPE_DECIMAL:     // null is ttint min for decimal
        case WIRE_TYPE_GEOGRAPHY:   // polygon with zero rings
        case WIRE_TYPE_GEOGRAPHY_POINT: //long 360 n lat 360
        default:
            assert(false);
        }
        return *this;
    }

    RowBuilder& addString(const std::string& val) {
        validateType(WIRE_TYPE_STRING);
        m_buffer.ensureRemaining(4 + static_cast<int32_t>(val.size()));
        m_buffer.putString(val);
        m_currentColumnIndex++;
        return *this;
    }

    RowBuilder& addVarbinary(const int32_t bufsize, const uint8_t *in_value) {
        validateType(WIRE_TYPE_VARBINARY);
        m_buffer.ensureRemaining(4 + bufsize);
        m_buffer.putBytes(bufsize, in_value);
        m_currentColumnIndex++;
        return *this;
    }

    RowBuilder& addTimeStamp(int64_t value) {
        validateType(WIRE_TYPE_TIMESTAMP);
        m_buffer.ensureRemaining(8);
        m_buffer.putInt64(value);
        m_currentColumnIndex++;
        return *this;
    }

    RowBuilder& addDecimal(const Decimal& value) {
        validateType(WIRE_TYPE_DECIMAL);
        m_buffer.ensureRemaining(2 * sizeof (int64_t));
        value.serializeTo(&m_buffer);
        m_currentColumnIndex++;
        return *this;
    }

    RowBuilder& addGeographyPoint(const GeographyPoint &val) {
        validateType(WIRE_TYPE_GEOGRAPHY_POINT);
        // One byte for the type and 2*sizeof(double) bytes
        // for the payload.
        m_buffer.ensureRemaining(2*sizeof(double));
        m_buffer.putDouble(val.getLongitude());
        m_buffer.putDouble(val.getLatitude());
        m_currentColumnIndex++;
        return *this;
    }

    RowBuilder& addGeography(const Geography &val) {
        validateType(WIRE_TYPE_GEOGRAPHY);
        int32_t valSize = val.getSerializedSize();
        m_buffer.ensureRemaining(1 + valSize);
        int realSize = val.serializeTo(m_buffer);
        m_currentColumnIndex++;
        return *this;
    }

    void reset() {
        m_buffer.clear();
        m_currentColumnIndex = 0;
    }

    /** Copies over row row data into provided byte buffer
     *  Precondition to calling this function: All the columns of the
     *  row schema have been populated
     */
    void serializeTo(ExpandableByteBuffer& buffer) {
        if (m_currentColumnIndex != m_columns.size()) {
            throw UninitializedColumnException(m_currentColumnIndex, m_columns.size());
        }

        m_buffer.flip();
        buffer.ensureRemaining(m_buffer.limit());
        buffer.put(&m_buffer);
        reset();
    }

    /** Returns number of first 'n'n populated columns for the given row.
     * For example 0 meaning none of the row columns are populated
     * 1 telling first column of the row has been populated.
     * Row does not support holes, so if return count is 3 it represents
     * all 3 columns from beginning have been populated
     */
    int32_t numberOfPopulatedColumns() const {
        return m_currentColumnIndex;
    }

    const std::vector<voltdb::Column>& getSchema() const { return m_columns; }
private:
    std::vector<voltdb::Column> m_columns;
    voltdb::ScopedByteBuffer m_buffer;
    uint32_t m_currentColumnIndex;
};
}

#endif /* VOLTDB_ROWBUILDER_H_ */
