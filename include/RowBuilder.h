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

#ifndef VOLTDB_ROWBUILDER_H_
#define VOLTDB_ROWBUILDER_H_
#include "ByteBuffer.hpp"
#include "WireType.h"
#include "Column.hpp"
#include "boost/shared_ptr.hpp"
#include <stdint.h>
#include "Exception.hpp"

namespace voltdb {
class ColumnMismatchException : public voltdb::Exception {
    const char * what() const throw() {
        return "Attempted to set a column using the wrong type";
    }
};
class Table;
class RowBuilder {
    friend class Table;
private:
    void validateType(WireType type) {
        if (m_columns[m_currentColumn].m_type != type ||
                m_currentColumn > m_columns.size()) {
            throw ColumnMismatchException();
        }
    }
public:
    RowBuilder(Table *table);

    void addInt64(int64_t val) {
        validateType(WIRE_TYPE_BIGINT);
        m_buffer.ensureRemaining(8);
        m_buffer.putInt64(val);
        m_currentColumn++;
    }
    void addInt32(int32_t val) {
        validateType(WIRE_TYPE_INTEGER);
        m_buffer.ensureRemaining(4);
        m_buffer.putInt32(val);
        m_currentColumn++;
    }
    void addInt16(int16_t val) {
        validateType(WIRE_TYPE_SMALLINT);
        m_buffer.ensureRemaining(2);
        m_buffer.putInt16(val);
        m_currentColumn++;
    }
    void addInt8(int8_t val) {
        validateType(WIRE_TYPE_TINYINT);
        m_buffer.ensureRemaining(1);
        m_buffer.putInt8(val);
        m_currentColumn++;
    }
    void addDouble(double val) {
        validateType(WIRE_TYPE_FLOAT);
        m_buffer.ensureRemaining(8);
        m_buffer.putDouble(val);
        m_currentColumn++;
    }
    void addNull() {
        if (m_currentColumn > m_columns.size()) {
            throw new ColumnMismatchException();
        }
        switch (m_columns[m_currentColumn].m_type) {
        case WIRE_TYPE_BIGINT:
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
            m_currentColumn++;
            break;
        case WIRE_TYPE_VARBINARY:
            m_buffer.ensureRemaining(4);
            m_buffer.putInt32(-1);
            m_currentColumn++;
            break;
        default:
            assert(false);
        }
    }
    void addString(const std::string& val) {
        validateType(WIRE_TYPE_STRING);
        m_buffer.ensureRemaining(4 + static_cast<int32_t>(val.size()));
        m_buffer.putString(val);
        m_currentColumn++;
    }

    void addVarbinary(const int32_t bufsize, const uint8_t *in_value) {
        validateType(WIRE_TYPE_VARBINARY);
        m_buffer.ensureRemaining(4 + bufsize);
        m_buffer.putBytes(bufsize, in_value);
        m_currentColumn++;
    }

    void reset() {
        m_buffer.clear();
        m_currentColumn = 0;
    }
private:
    std::vector<voltdb::Column> m_columns;
    voltdb::ScopedByteBuffer m_buffer;
    uint32_t m_currentColumn;
};
}

#endif /* VOLTDB_ROWBUILDER_H_ */
