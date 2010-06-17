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

#ifndef VOLTDB_PARAMETERSET_HPP_
#define VOLTDB_PARAMETERSET_HPP_
#include "Parameter.hpp"
#include "RowBuilder.h"
#include <vector>
#include "ByteBuffer.hpp"
#include "boost/shared_ptr.hpp"
#include "Exception.hpp"
#include "Decimal.hpp"

namespace voltdb {
class Procedure;
class ParameterSet {
    friend class Procedure;
private:
    ParameterSet(std::vector<Parameter> parameters) : m_parameters(parameters), m_buffer(8192), m_currentParam(0) {
        m_buffer.putInt16(static_cast<int16_t>(m_parameters.size()));
    }
    void validateType(WireType type, bool isArray) {
        if (m_parameters[m_currentParam].m_type != type ||
                m_currentParam > m_parameters.size() ||
                m_parameters[m_currentParam].m_array != isArray) {
            throw ParamMismatchException();
        }
    }
public:

    ParameterSet& addDecimal(Decimal val) {
        validateType(WIRE_TYPE_DECIMAL, false);
        m_buffer.ensureRemaining(sizeof(Decimal) + 1);
        m_buffer.putInt8(WIRE_TYPE_DECIMAL);
        val.serializeTo(&m_buffer);
        m_currentParam++;
        return *this;
    }

    ParameterSet& addDecimal(std::vector<Decimal> vals) {
        validateType(WIRE_TYPE_DECIMAL, true);
        m_buffer.ensureRemaining(4 + (sizeof(Decimal) * vals.size()));
        m_buffer.putInt8(WIRE_TYPE_ARRAY);
        m_buffer.putInt8(WIRE_TYPE_DECIMAL);
        m_buffer.putInt16(static_cast<int16_t>(vals.size()));
        for (std::vector<Decimal>::iterator i = vals.begin(); i != vals.end(); i++) {
            i->serializeTo(&m_buffer);
        }
        m_currentParam++;
        return *this;
    }

    ParameterSet& addTimestamp(int64_t val) {
        validateType(WIRE_TYPE_TIMESTAMP, false);
        m_buffer.ensureRemaining(9);
        m_buffer.putInt8(WIRE_TYPE_TIMESTAMP);
        m_buffer.putInt64(val);
        m_currentParam++;
        return *this;
    }

    ParameterSet& addTimestamp(std::vector<int64_t> vals) {
        validateType(WIRE_TYPE_TIMESTAMP, true);
        m_buffer.ensureRemaining(4 + (sizeof(int64_t) * vals.size()));
        m_buffer.putInt8(WIRE_TYPE_ARRAY);
        m_buffer.putInt8(WIRE_TYPE_TIMESTAMP);
        m_buffer.putInt16(static_cast<int16_t>(vals.size()));
        for (std::vector<int64_t>::iterator i = vals.begin(); i != vals.end(); i++) {
            m_buffer.putInt64(*i);
        }
        m_currentParam++;
        return *this;
    }

    ParameterSet& addInt64(int64_t val) {
        validateType(WIRE_TYPE_BIGINT, false);
        m_buffer.ensureRemaining(9);
        m_buffer.putInt8(WIRE_TYPE_BIGINT);
        m_buffer.putInt64(val);
        m_currentParam++;
        return *this;
    }

    ParameterSet& addInt64(std::vector<int64_t> vals) {
        validateType(WIRE_TYPE_BIGINT, true);
        m_buffer.ensureRemaining(4 + (sizeof(int64_t) * vals.size()));
        m_buffer.putInt8(WIRE_TYPE_ARRAY);
        m_buffer.putInt8(WIRE_TYPE_BIGINT);
        m_buffer.putInt16(static_cast<int16_t>(vals.size()));
        for (std::vector<int64_t>::iterator i = vals.begin(); i != vals.end(); i++) {
            m_buffer.putInt64(*i);
        }
        m_currentParam++;
        return *this;
    }

    ParameterSet& addInt32(int32_t val) {
        validateType(WIRE_TYPE_INTEGER, false);
        m_buffer.ensureRemaining(5);
        m_buffer.putInt8(WIRE_TYPE_INTEGER);
        m_buffer.putInt32(val);
        m_currentParam++;
        return *this;
    }

    ParameterSet& addInt32(std::vector<int32_t> vals) {
        validateType(WIRE_TYPE_INTEGER, true);
        m_buffer.ensureRemaining(4 + (sizeof(int32_t) * vals.size()));
        m_buffer.putInt8(WIRE_TYPE_ARRAY);
        m_buffer.putInt8(WIRE_TYPE_INTEGER);
        m_buffer.putInt16(static_cast<int16_t>(vals.size()));
        for (std::vector<int32_t>::iterator i = vals.begin(); i != vals.end(); i++) {
            m_buffer.putInt32(*i);
        }
        m_currentParam++;
        return *this;
    }

    ParameterSet& addInt16(int16_t val) {
        validateType(WIRE_TYPE_SMALLINT, false);
        m_buffer.ensureRemaining(3);
        m_buffer.putInt8(WIRE_TYPE_SMALLINT);
        m_buffer.putInt16(val);
        m_currentParam++;
        return *this;
    }

    ParameterSet& addInt16(std::vector<int16_t> vals) {
        validateType(WIRE_TYPE_SMALLINT, true);
        m_buffer.ensureRemaining(4 + (sizeof(int16_t) * vals.size()));
        m_buffer.putInt8(WIRE_TYPE_ARRAY);
        m_buffer.putInt8(WIRE_TYPE_SMALLINT);
        m_buffer.putInt16(static_cast<int16_t>(vals.size()));
        for (std::vector<int16_t>::iterator i = vals.begin(); i != vals.end(); i++) {
            m_buffer.putInt16(*i);
        }
        m_currentParam++;
        return *this;
    }

    ParameterSet& addInt8(int8_t val) {
        validateType(WIRE_TYPE_TINYINT, false);
        m_buffer.ensureRemaining(2);
        m_buffer.putInt8(WIRE_TYPE_TINYINT);
        m_buffer.putInt8(val);
        m_currentParam++;
        return *this;
    }

    ParameterSet& addInt8(std::vector<int8_t> vals) {
        validateType(WIRE_TYPE_TINYINT, true);
        m_buffer.ensureRemaining(6 + (sizeof(int8_t) * vals.size()));
        m_buffer.putInt8(WIRE_TYPE_ARRAY);
        m_buffer.putInt8(WIRE_TYPE_TINYINT);
        m_buffer.putInt32(static_cast<int32_t>(vals.size()));
        for (std::vector<int8_t>::iterator i = vals.begin(); i != vals.end(); i++) {
            m_buffer.putInt8(*i);
        }
        m_currentParam++;
        return *this;
    }

    ParameterSet& addDouble(double val) {
        validateType(WIRE_TYPE_FLOAT, false);
        m_buffer.ensureRemaining(9);
        m_buffer.putInt8(WIRE_TYPE_FLOAT);
        m_buffer.putDouble(val);
        m_currentParam++;
        return *this;
    }

    ParameterSet& addDouble(std::vector<double> vals) {
        validateType(WIRE_TYPE_FLOAT, true);
        m_buffer.ensureRemaining(2 + (sizeof(double) * vals.size()));
        m_buffer.putInt8(WIRE_TYPE_ARRAY);
        m_buffer.putInt8(WIRE_TYPE_FLOAT);
        m_buffer.putInt16(static_cast<int16_t>(vals.size()));
        for (std::vector<double>::iterator i = vals.begin(); i != vals.end(); i++) {
            m_buffer.putDouble(*i);
        }
        m_currentParam++;
        return *this;
    }

    ParameterSet& addNull() {
        if (m_currentParam > m_parameters.size()) {
            throw new ParamMismatchException();
        }
        m_buffer.ensureRemaining(1);
        m_buffer.putInt8(WIRE_TYPE_NULL);
        m_currentParam++;
        return *this;
    }

    ParameterSet& addString(std::string val) {
        validateType(WIRE_TYPE_STRING, false);
        m_buffer.ensureRemaining(5 + val.size());
        m_buffer.putInt8(WIRE_TYPE_STRING);
        m_buffer.putString(val);
        m_currentParam++;
        return *this;
    }
    ParameterSet& addString(std::vector<std::string> vals) {
        validateType(WIRE_TYPE_STRING, true);
        int32_t totalStringLength = 0;
        for (std::vector<std::string>::iterator i = vals.begin(); i != vals.end(); i++) {
            totalStringLength += static_cast<int32_t>((*i).size());
        }
        m_buffer.ensureRemaining(4 +
                totalStringLength +
                (4 * static_cast<int32_t>(vals.size())));
        m_buffer.putInt8(WIRE_TYPE_ARRAY);
        m_buffer.putInt8(WIRE_TYPE_STRING);
        m_buffer.putInt16(static_cast<int16_t>(vals.size()));
        for (std::vector<std::string>::iterator i = vals.begin(); i != vals.end(); i++) {
            m_buffer.putString(*i);
        }
        m_currentParam++;
        return *this;
    }
    void reset() {
        m_buffer.clear();
        m_currentParam = 0;
        m_buffer.putInt16(static_cast<int16_t>(m_parameters.size()));
    }

    int32_t getSerializedSize() {
        if (m_currentParam != m_parameters.size()) {
            throw UninitializedParamsException();
        }
        return m_buffer.position();
    }
    void serializeTo(ByteBuffer *buffer) {
        if (m_currentParam != m_parameters.size()) {
            throw UninitializedParamsException();
        }
        m_buffer.flip();
        buffer->put(&m_buffer);
        reset();
    }
private:
    std::vector<Parameter> m_parameters;
    ScopedByteBuffer m_buffer;
    uint32_t m_currentParam;
};
}
#endif /* VOLTDB_PARAMETERSET_HPP_ */
