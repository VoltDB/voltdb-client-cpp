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

#ifndef VOLTDB_ROW_HPP_
#define VOLTDB_ROW_HPP_

#include "ByteBuffer.hpp"
#include "Column.hpp"
#include <string>
#include <vector>
#include "Exception.hpp"
#include "WireType.h"
#include <stdint.h>
#include "Decimal.hpp"
#include <sstream>

namespace voltdb {

/*
 * Representation of a row of tabular data from a Table. Every row contains a shared pointer
 * to its origin table which may also contain a shared pointer to the message the table was originally
 * from. Retain references with care to avoid memory leaks.
 */
class Row {
public:
    /*
     * Construct a row from a buffer containing the row data.
     */
#ifdef SWIG
    %ignore Row(SharedByteBuffer rowData, boost::shared_ptr<std::vector<voltdb::Column> > columns);
#endif
    Row(SharedByteBuffer rowData, boost::shared_ptr<std::vector<voltdb::Column> > columns) :
        m_data(rowData), m_columns(columns), m_wasNull(false), m_offsets(columns->size()), m_hasCalculatedOffsets(false) {
    }

    /*
     * Retrieve the value at the specified column index as bytes. The type of the column
     * must be Varbinary.
     * @throws InvalidColumnException The index of the column was invalid or the type of the column does
     * not match the type of the get method.
     * @return Whether the buffer provided was large enough.
     */
    bool getVarbinary(errType& err, int32_t column, int32_t bufsize, uint8_t *out_value, int32_t *out_len) {
        validateType(WIRE_TYPE_VARBINARY, column);
        return m_data.getBytes(err, getOffset(column), m_wasNull, bufsize, out_value, out_len);
    }

    /*
     * Retrieve the value at the specified column index as a Decimal. The type of the column
     * must be Decimal.
     * @throws InvalidColumnException The index of the column was invalid or the type of the column does
     * not match the type of the get method.
     * @return Decimal value at the specified column
     */
    Decimal getDecimal(errType& err, int32_t column) {
        validateType(WIRE_TYPE_DECIMAL, column);
        char data[16];
        m_data.get(err, getOffset(column), data, 16);
        Decimal retval(data);
        m_wasNull = retval.isNull();
        return retval;
    }

    /*
     * Retrieve the value at the specified column index as a Timestamp. The type of the column
     * must be Timestamp.
     * @throws InvalidColumnException The index of the column was invalid or the type of the column does
     * not match the type of the get method.
     * @return Timestamp value at the specified column
     */
    int64_t getTimestamp(errType& err, int32_t column) {
        validateType(WIRE_TYPE_TIMESTAMP, column);
        int64_t retval = m_data.getInt64(err, getOffset(column));
        if (retval == INT64_MIN) m_wasNull = true;
        return retval;
    }

    /*
     * Retrieve the value at the specified column index as an int64_t/BIGINT. The type of the column
     * must be BIGINT, INTEGER, SMALLINT, or TINYINT.
     * @throws InvalidColumnException The index of the column was invalid or the type of the column does
     * not match the type of the get method.
     * @return int64 value at the specified column
     */
    int64_t getInt64(errType& err, int32_t column) {
        WireType type = validateType(WIRE_TYPE_BIGINT, column);
        int64_t retval;
        switch (type) {
        case WIRE_TYPE_BIGINT:
            retval = m_data.getInt64(err, getOffset(column));
            if (retval == INT64_MIN) m_wasNull = true;
            break;
        case WIRE_TYPE_INTEGER:
            retval = m_data.getInt32(err, getOffset(column));
            if (retval == INT32_MIN) m_wasNull = true;
            break;
        case WIRE_TYPE_SMALLINT:
            retval = m_data.getInt16(err, getOffset(column));
            if (retval == INT16_MIN) m_wasNull = true;
            break;
        case WIRE_TYPE_TINYINT:
            retval = m_data.getInt8(err, getOffset(column));
            if (retval == INT8_MIN) m_wasNull = true;
            break;
        default:
            assert(false);
        }
        return retval;
    }

    /*
     * Retrieve the value at the specified column index as an int32_t/INTEGER. The type of the column
     * must be INTEGER, SMALLINT, or TINYINT.
     * @throws InvalidColumnException The index of the column was invalid or the type of the column does
     * not match the type of the get method.
     * @return int32 value at the specified column
     */
    int32_t getInt32(errType& err, int32_t column) {
        WireType type = validateType(WIRE_TYPE_INTEGER, column);
        int32_t retval;
        switch (type) {
        case WIRE_TYPE_INTEGER:
            retval = m_data.getInt32(errType& err, getOffset(column));
            if (retval == INT32_MIN) m_wasNull = true;
            break;
        case WIRE_TYPE_SMALLINT:
            retval = m_data.getInt16(errType& err, getOffset(column));
            if (retval == INT16_MIN) m_wasNull = true;
            break;
        case WIRE_TYPE_TINYINT:
            retval = m_data.getInt8(errType& err, getOffset(column));
            if (retval == INT8_MIN) m_wasNull = true;
            break;
        default:
            assert(false);
        }
        return retval;
    }

    /*
     * Retrieve the value at the specified column index as an int16_t/SMALLINT. The type of the column
     * must be SMALLINT, or TINYINT.
     * @throws InvalidColumnException The index of the column was invalid or the type of the column does
     * not match the type of the get method.
     * @return int16 value at the specified column
     */
    int16_t getInt16(errType& err, int32_t column) {
        WireType type = validateType(WIRE_TYPE_SMALLINT, column);
        int16_t retval;
        switch (type) {
        case WIRE_TYPE_SMALLINT:
            retval = m_data.getInt16(err, getOffset(column));
            if (retval == INT16_MIN) m_wasNull = true;
            break;
        case WIRE_TYPE_TINYINT:
            retval = m_data.getInt8(err, getOffset(column));
            if (retval == INT8_MIN) m_wasNull = true;
            break;
        default:
            assert(false);
        }
        return retval;
    }

    /*
     * Retrieve the value at the specified column index as an int8_t/TINYINT. The type of the column
     * must be TINYINT.
     * @throws InvalidColumnException The index of the column was invalid or the type of the column does
     * not match the type of the get method.
     * @return int8 value at the specified column
     */
    int8_t getInt8(errType& err, int32_t column) {
        validateType(WIRE_TYPE_TINYINT, column);
        int8_t retval = m_data.getInt8(err, getOffset(column));
        if (retval == INT8_MIN) {
            m_wasNull = true;
        }
        return retval;
    }

    /*
     * Retrieve the value at the specified column index as a double. The type of the column
     * must be FLOAT.
     * @throws InvalidColumnException The index of the column was invalid or the type of the column does
     * not match the type of the get method.
     * @return double value at the specified column
     */
    double getDouble(errType& err, int32_t column) {
        validateType(WIRE_TYPE_FLOAT, column);
        double retval = m_data.getDouble(err, getOffset(column));
        if (retval <= -1.7E+308) {
            m_wasNull = true;
        }
        return retval;
    }

    /*
     * Retrieve the value at the specified column index as an string. The type of the column
     * must be STRING.
     * @throws InvalidColumnException The index of the column was invalid or the type of the column does
     * not match the type of the get method.
     * @return String value at the specified column
     */
    std::string getString(errType& err, int32_t column) {
        validateType(WIRE_TYPE_STRING, column);
        return m_data.getString(err, getOffset(column), m_wasNull);
    }

    /*
     * Returns true if the value at the specified column index is NULL and false otherwise
     * @throws InvalidColumnException The name of the column was invalid.
     * @return true if the value is NULL and false otherwise
     */
    bool isNull(errType& err, int32_t column) {
        if (column < 0 || column >= static_cast<ssize_t>(m_columns->size())) {
            throw InvalidColumnException();
        }
        WireType columnType = m_columns->at(static_cast<size_t>(column)).m_type;
        switch (columnType) {
        case WIRE_TYPE_DECIMAL:
            getDecimal(err, column); break;
        case WIRE_TYPE_TIMESTAMP:
            getTimestamp(err, column); break;
        case WIRE_TYPE_BIGINT:
            getInt64(err, column); break;
        case WIRE_TYPE_INTEGER:
            getInt32(err, column); break;
        case WIRE_TYPE_SMALLINT:
            getInt64(err, column); break;
        case WIRE_TYPE_TINYINT:
            getInt8(err, column); break;
        case WIRE_TYPE_FLOAT:
            getDouble(err, column); break;
        case WIRE_TYPE_STRING:
            getString(err, column); break;
        case WIRE_TYPE_VARBINARY:
            int out_len;
            getVarbinary(err, column, 0, NULL, &out_len); break;
        default:
            assert(false);
        }
        return wasNull();
    }

    /*
     * Retrieve the value from the column with the specified name as bytes. The type of the column
     * must be Varbinary.
     * @throws InvalidColumnException The index of the column was invalid or the type of the column does
     * not match the type of the get method.
     * @return Whether the buffer provided was large enough.
     */
    bool getVarbinary(errType& err, std::string cname, int32_t bufsize, uint8_t *out_value, int32_t *out_len)
    {
        return getVarbinary(err, getColumnIndexByName(cname), bufsize, out_value, out_len);
    }

    /*
     * Retrieve the value from the column with the specified name as a Decimal. The type of the column
     * must be Decimal.
     * @throws InvalidColumnException No column with the specified name exists or the type of the getter
     * does not match the column type.
     * @return Decimal value at the specified column
     */
    Decimal getDecimal(errType& err, std::string cname) {
        return getDecimal(err, getColumnIndexByName(cname));
    }

    /*
     * Retrieve the value from the column with the specified name as a Timestamp. The type of the column
     * must be Timestamp.
     * @throws InvalidColumnException No column with the specified name exists or the type of the getter
     * does not match the column type.
     * @return Timestamp value at the specified column
     */
    int64_t getTimestamp(errType& err, std::string cname) {
        return getTimestamp(err, getColumnIndexByName(cname));
    }

    /*
     * Retrieve the value from the column with the specified name as a int64_t/BIGINT. The type of the column
     * must be BIGINT, INTEGER, SMALLINT, TINYINT.
     * @throws InvalidColumnException No column with the specified name exists or the type of the getter
     * does not match the column type.
     * @return int64 value at the specified column
     */
    int64_t getInt64(errType& err, std::string cname) {
        return getInt64(err, getColumnIndexByName(cname));
    }

    /*
     * Retrieve the value from the column with the specified name as a int32_t/INTEGER. The type of the column
     * must be INTEGER, SMALLINT, TINYINT.
     * @throws InvalidColumnException No column with the specified name exists or the type of the getter
     * does not match the column type.
     * @return int32 value at the specified column
     */
    int32_t getInt32(errType& err, std::string cname) {
        return getInt32(err, getColumnIndexByName(cname));
    }

    /*
     * Retrieve the value from the column with the specific name as a int16_t/SMALLINT. The type of the column
     * must be SMALLINT, TINYINT.
     * @throws InvalidColumnException No column with the specified name exists or the type of the getter
     * does not match the column type.
     * @return int16 value at the specified column
     */
    int16_t getInt16(errType& err, std::string cname) {
        return getInt16(err, getColumnIndexByName(cname));
    }

    /*
     * Retrieve the value from the column with the specific name as a int8_t/TINYINT. The type of the column
     * must be TINYINT.
     * @throws InvalidColumnException No column with the specified name exists or the type of the getter
     * does not match the column type.
     * @return int8 value at the specified column
     */
    int8_t getInt8(errType& err, std::string cname) {
        return getInt8(err, getColumnIndexByName(cname));
    }

    /*
     * Retrieve the value from the column with the specific name as a double. The type of the column
     * must be FLOAT.
     * @throws InvalidColumnException No column with the specified name exists or the type of the getter
     * does not match the column type.
     * @return double value at the specified column
     */
    double getDouble(errType &err, std::string cname) {
        return getDouble(err, getColumnIndexByName(cname));
    }

    /*
     * Retrieve the value from the column with the specific name as a std::string. The type of the column
     * must be STRING.
     * @throws InvalidColumnException No column with the specified name exists or the type of the getter
     * does not match the column type.
     * @return string value at the specified column
     */
    std::string getString(errType& err, std::string cname) {
        return getString(err, getColumnIndexByName(cname));
    }

    /*
     * Returns true if the value in the column with the specified name is NULL and false otherwise
     * @throws InvalidColumnException The name of the column was invalid.
     * @return true if the value is NULL and false otherwise
     */
    bool isNull(errType, std::string cname) {
        int32_t column = getColumnIndexByName(cname);
        return isNull(err, column);
    }

    /*
     * Returns true if the value of the last column returned via a getter was null
     */
    bool wasNull() {
        return m_wasNull;
    }

    /*
     * Returns a string representation of this row
     */
    std::string toString() {
        std::ostringstream ostream;
        toString(ostream, std::string(""));
        return ostream.str();
    }

    /*
     * Returns a string reprentation of this row with the specified level of indentation
     * before each line
     */
    void toString(errType &err, std::ostringstream &ostream, std::string indent) {
        ostream << indent;
        const int32_t size = static_cast<int32_t>(m_columns->size());
        for (int32_t ii = 0; ii < size; ii++) {
            if (ii != 0) {
                ostream << ", ";
            }
            if (isNull(err, ii)) {
                ostream << "NULL";
                continue;
            }
            switch(m_columns->at(ii).m_type) {
            case WIRE_TYPE_TINYINT:
                ostream << static_cast<int32_t>(getInt8(err, ii)); break;
            case WIRE_TYPE_SMALLINT:
                ostream << getInt16(err, ii); break;
            case WIRE_TYPE_INTEGER:
                ostream << getInt32(err, ii); break;
            case WIRE_TYPE_BIGINT:
                ostream << getInt64(err, ii); break;
            case WIRE_TYPE_FLOAT:
                ostream << getDouble(err, ii); break;
            case WIRE_TYPE_STRING:
                ostream << "\"" << getString(err, ii) << "\""; break;
            case WIRE_TYPE_TIMESTAMP:
                ostream << getTimestamp(err, ii); break;
            case WIRE_TYPE_DECIMAL:
                ostream << getDecimal(err, ii).toString(); break;
            case WIRE_TYPE_VARBINARY:
                ostream << "VARBINARY VALUE"; break;
            default:
                assert(false);
            }
        }
    }

    int32_t columnCount() {
        return static_cast<int32_t>(m_columns->size());
    }

    std::vector<voltdb::Column> columns() {
        return *m_columns;
    }
private:
    WireType validateType(WireType type, int32_t index)  {
        if (index < 0 ||
                index >= static_cast<ssize_t>(m_columns->size())) {
            throw InvalidColumnException();
        }
        WireType columnType = m_columns->at(static_cast<size_t>(index)).m_type;
        switch (columnType) {
        case WIRE_TYPE_DECIMAL:
            if (type != WIRE_TYPE_DECIMAL) throw InvalidColumnException();
            break;
        case WIRE_TYPE_TIMESTAMP:
            if (type != WIRE_TYPE_TIMESTAMP) throw InvalidColumnException();
            break;
        case WIRE_TYPE_BIGINT:
            if (type != WIRE_TYPE_BIGINT) throw InvalidColumnException();
            break;
        case WIRE_TYPE_INTEGER:
            if (type != WIRE_TYPE_BIGINT && type != WIRE_TYPE_INTEGER) throw InvalidColumnException();
            break;
        case WIRE_TYPE_SMALLINT:
            if (type != WIRE_TYPE_BIGINT &&
                    type != WIRE_TYPE_INTEGER &&
                    type != WIRE_TYPE_SMALLINT) throw InvalidColumnException();
            break;
        case WIRE_TYPE_TINYINT:
            if (type != WIRE_TYPE_BIGINT &&
                    type != WIRE_TYPE_INTEGER &&
                    type != WIRE_TYPE_SMALLINT &&
                    type != WIRE_TYPE_TINYINT) throw InvalidColumnException();
            break;
        case WIRE_TYPE_FLOAT:
            if (type != WIRE_TYPE_FLOAT) throw InvalidColumnException();
            break;
        case WIRE_TYPE_STRING:
            if (type != WIRE_TYPE_STRING) throw InvalidColumnException();
            break;
        case WIRE_TYPE_VARBINARY:
            if (type != WIRE_TYPE_VARBINARY) throw InvalidColumnException();
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
            WireType type = m_columns->at(static_cast<size_t>(i - 1)).m_type;
            if (type == WIRE_TYPE_STRING || (type == WIRE_TYPE_VARBINARY)) {
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
                case WIRE_TYPE_DECIMAL:
                    length = 16;
                    break;
                case WIRE_TYPE_TIMESTAMP:
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

    SharedByteBuffer m_data;
    boost::shared_ptr<std::vector<voltdb::Column> > m_columns;
    bool m_wasNull;
    std::vector<int32_t> m_offsets;
    bool m_hasCalculatedOffsets;
};
}

#endif /* VOLTDB_ROW_HPP_ */
