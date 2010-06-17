/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

#ifndef ROW_HPP_
#define ROW_HPP_

#include "ByteBuffer.hpp"
#include "Column.hpp"
#include <string>
#include <vector>
#include "Exception.hpp"
#include "WireType.h"
#include <stdint.h>

namespace voltdb {
class InvalidColumnException : public voltdb::Exception {
    const char * what() const throw() {
        return "Attempted to retrieve a column with an invalid index or name, or an invalid type for the specified column";
    }
};

class Row {
private:
    WireType validateType(WireType type, int32_t index)  throw (InvalidColumnException) {
        if (index < 0 ||
                index >= static_cast<ssize_t>(m_columns->size())) {
            throw InvalidColumnException();
        }
        WireType columnType = m_columns->at(static_cast<size_t>(index)).m_type;
        switch (columnType) {
        case WIRE_TYPE_BIGINT:
            if (type != WIRE_TYPE_BIGINT) throw InvalidColumnException();
            break;
        case WIRE_TYPE_INTEGER:
            if (type != WIRE_TYPE_BIGINT || type != WIRE_TYPE_INTEGER) throw InvalidColumnException();
            break;
        case WIRE_TYPE_SMALLINT:
            if (type != WIRE_TYPE_BIGINT ||
                    type != WIRE_TYPE_INTEGER ||
                    type != WIRE_TYPE_SMALLINT) throw InvalidColumnException();
            break;
        case WIRE_TYPE_TINYINT:
            if (type != WIRE_TYPE_BIGINT ||
                    type != WIRE_TYPE_INTEGER ||
                    type != WIRE_TYPE_SMALLINT ||
                    type != WIRE_TYPE_TINYINT) throw InvalidColumnException();
            break;
        case WIRE_TYPE_FLOAT:
            if (type != WIRE_TYPE_FLOAT) throw InvalidColumnException();
            break;
        case WIRE_TYPE_STRING:
            if (type != WIRE_TYPE_STRING) throw InvalidColumnException();
            break;
        default:
            assert(false);
            break;
        }
        return columnType;
    }

    int32_t getColumnIndexByName(std::string name) {
        for (int32_t ii = 0; ii < static_cast<ssize_t>(m_columns->size()); ii++) {
            if (m_columns->at(static_cast<size_t>(ii)).m_name == name) {
                return ii;
            }
        }
        throw InvalidColumnException();
    }

    void ensureCalculatedOffsets() {
        if (m_hasCalculatedOffsets == true) return;
        m_offsets[0] = m_data.position();
        for (int32_t i = 1; i < static_cast<ssize_t>(m_columns->size()); i++) {
            WireType type = m_columns->at(static_cast<size_t>(i)).m_type;
            if (type == WIRE_TYPE_STRING) {
                int32_t length = m_data.getInt32(m_offsets[static_cast<size_t>(i - 1)]);
                if (length == -1) {
                    m_offsets[static_cast<size_t>(i)] = m_offsets[static_cast<size_t>(i - 1)] + 4;
                } else if (length < 0) {
                    assert(false);
                } else {
                    m_offsets[static_cast<size_t>(i)] = m_offsets[static_cast<size_t>(i - 1)] + length + 4;
                }
            } else {
                int32_t length = 0;
                switch (type) {
                case WIRE_TYPE_BIGINT:
                case WIRE_TYPE_FLOAT:
                    length = 8;
                    break;
                case WIRE_TYPE_INTEGER:
                    length = 4;
                    break;
                case WIRE_TYPE_SMALLINT:
                    length = 2;
                    break;
                case WIRE_TYPE_TINYINT:
                    length = 1;
                    break;
                default:
                    assert(false);
                }
                m_offsets[static_cast<size_t>(i)] = m_offsets[static_cast<size_t>(i - 1)] + length;
            }
        }
        m_hasCalculatedOffsets = true;
    }

    int32_t getOffset(int32_t index) {
        m_wasNull = false;
        ensureCalculatedOffsets();
        assert(index >= 0);
        assert(static_cast<size_t>(index) < m_offsets.size());
        return m_offsets[static_cast<size_t>(index)];
    }

public:
    Row(SharedByteBuffer rowData, boost::shared_ptr<std::vector<voltdb::Column> > columns) :
        m_data(rowData), m_columns(columns), m_wasNull(false), m_offsets(columns->size()), m_hasCalculatedOffsets(false) {
    }

    int64_t getInt64(int32_t column) {
        WireType type = validateType(WIRE_TYPE_BIGINT, column);
        int64_t retval;
        switch (type) {
        case WIRE_TYPE_BIGINT:
            retval = m_data.getInt64(getOffset(column));
            if (retval == INT64_MIN) m_wasNull = true;
            break;
        case WIRE_TYPE_INTEGER:
            retval = m_data.getInt32(getOffset(column));
            if (retval == INT32_MIN) m_wasNull = true;
            break;
        case WIRE_TYPE_SMALLINT:
            retval = m_data.getInt16(getOffset(column));
            if (retval == INT16_MIN) m_wasNull = true;
            break;
        case WIRE_TYPE_TINYINT:
            retval = m_data.getInt8(getOffset(column));
            if (retval == INT8_MIN) m_wasNull = true;
            break;
        default:
            assert(false);
        }
        return retval;
    }

    int32_t getInt32(int32_t column) {
        WireType type = validateType(WIRE_TYPE_INTEGER, column);
        int32_t retval;
        switch (type) {
        case WIRE_TYPE_INTEGER:
            retval = m_data.getInt32(getOffset(column));
            if (retval == INT32_MIN) m_wasNull = true;
            break;
        case WIRE_TYPE_SMALLINT:
            retval = m_data.getInt16(getOffset(column));
            if (retval == INT16_MIN) m_wasNull = true;
            break;
        case WIRE_TYPE_TINYINT:
            retval = m_data.getInt8(getOffset(column));
            if (retval == INT8_MIN) m_wasNull = true;
            break;
        default:
            assert(false);
        }
        return retval;
    }

    int16_t getInt16(int32_t column) {
        WireType type = validateType(WIRE_TYPE_SMALLINT, column);
        int16_t retval;
        switch (type) {
        case WIRE_TYPE_SMALLINT:
            retval = m_data.getInt16(getOffset(column));
            if (retval == INT16_MIN) m_wasNull = true;
            break;
        case WIRE_TYPE_TINYINT:
            retval = m_data.getInt8(getOffset(column));
            if (retval == INT8_MIN) m_wasNull = true;
            break;
        default:
            assert(false);
        }
        return retval;
    }

    int8_t getInt8(int32_t column) {
        validateType(WIRE_TYPE_TINYINT, column);
        int8_t retval = m_data.getInt8(getOffset(column));
        if (retval == INT8_MIN) {
            m_wasNull = true;
        }
        return retval;
    }

    double getDouble(int32_t column) {
        validateType(WIRE_TYPE_FLOAT, column);
        double retval = m_data.getDouble(getOffset(column));
        if (retval < -1.7976931348623157E+308) {
            m_wasNull = true;
        }
        return retval;
    }

    std::string getString(int32_t column) {
        validateType(WIRE_TYPE_STRING, column);
        return m_data.getString(getOffset(column), m_wasNull);
    }

    bool isNull(int32_t column) {
        if (column < 0 || column >= static_cast<ssize_t>(m_columns->size())) {
            throw InvalidColumnException();
        }
        WireType columnType = m_columns->at(static_cast<size_t>(column)).m_type;
        switch (columnType) {
        case WIRE_TYPE_BIGINT:
            getInt64(column); break;
        case WIRE_TYPE_INTEGER:
            getInt32(column); break;
        case WIRE_TYPE_SMALLINT:
            getInt64(column); break;
        case WIRE_TYPE_TINYINT:
            m_data.getInt8(getOffset(column)); break;
        case WIRE_TYPE_FLOAT:
            m_data.getDouble(getOffset(column)); break;
        case WIRE_TYPE_STRING:
            m_data.getString(getOffset(column), m_wasNull); break;
        default:
            assert(false);
        }
        return wasNull();
    }

    int64_t getInt64(std::string cname, int64_t val) {
        return getInt64(getColumnIndexByName(cname));
    }

    int32_t getInt32(std::string cname, int32_t val) {
        return getInt32(getColumnIndexByName(cname));
    }

    int16_t getInt16(std::string cname, int16_t val) {
        return getInt16(getColumnIndexByName(cname));
    }

    int8_t getInt8(std::string cname, int8_t val) {
        return getInt8(getColumnIndexByName(cname));
    }

    double getDouble(std::string cname, double val) {
        return getDouble(getColumnIndexByName(cname));
    }

    std::string getString(std::string cname) {
        return getString(getColumnIndexByName(cname));
    }

    bool isNull(std::string cname) {
        int32_t column = getColumnIndexByName(cname);
        return isNull(column);
    }

    bool wasNull() {
        return m_wasNull;
    }
private:
    SharedByteBuffer m_data;
    boost::shared_ptr<std::vector<voltdb::Column> > m_columns;
    bool m_wasNull;
    std::vector<int32_t> m_offsets;
    bool m_hasCalculatedOffsets;
};
}

#endif /* ROW_HPP_ */
