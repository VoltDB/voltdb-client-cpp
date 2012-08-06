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

/*
 * Class for setting the parameters to a stored procedure one at a time. Parameters must be set from first
 * to last, and every parameter must set for each invocation.
 */
class ParameterSet {
    friend class Procedure;
public:

    /**
     * Add a binary payload
     * @throws ParamMismatchException Supplied parameter is the wrong type for this position or too many have been set
     * @return Reference to this parameter set to allow invocation chaining.
     */
    ParameterSet& addBytes(errType& err, const int32_t bufsize, const uint8_t *in_value)
    {
        validateType(WIRE_TYPE_VARBINARY, false);
        m_buffer.ensureRemaining(1 + 4 + bufsize);
        m_buffer.putInt8(err, WIRE_TYPE_VARBINARY);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer.putBytes(err, bufsize, in_value);
        if (!isOk(err)) {
            return *this;
        }
        m_currentParam++;
        return *this;
    }

    /**
     * Add a decimal value for the current parameter
     * @throws ParamMismatchException Supplied parameter is the wrong type for this position or too many have been set
     * @return Reference to this parameter set to allow invocation chaining.
     */
    ParameterSet& addDecimal(errType& err, Decimal val) {
        validateType(WIRE_TYPE_DECIMAL, false);
        m_buffer.ensureRemaining(static_cast<int32_t>(sizeof(Decimal)) + 1);
        m_buffer.putInt8(err, WIRE_TYPE_DECIMAL);
        if (!isOk(err)) {
            return *this;
        }
        val.serializeTo(err, &m_buffer);
        if (!isOk(err)) {
            return *this;
        }
        m_currentParam++;
        return *this;
    }

    /**
     * Add an array of decimal values for the current parameter
     * @throws ParamMismatchException Supplied parameter is the wrong type for this position or too many have been set
     * @return Reference to this parameter set to allow invocation chaining.
     */
    ParameterSet& addDecimal(errType& err, std::vector<Decimal> vals) {
        validateType(WIRE_TYPE_DECIMAL, true);
        m_buffer.ensureRemaining(4 + static_cast<int32_t>(sizeof(Decimal) * vals.size()));
        if (!isOk(err)) {
            return *this;
        }
        m_buffer.putInt8(err, WIRE_TYPE_ARRAY);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer.putInt8(err, WIRE_TYPE_DECIMAL);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer.putInt16(err, static_cast<int16_t>(vals.size()));
        if (!isOk(err)) {
            return *this;
        }
        for (std::vector<Decimal>::iterator i = vals.begin(); i != vals.end(); i++) {
            i->serializeTo(err, &m_buffer);
            if (!isOk(err)) {
                return *this;
            }
        }
        m_currentParam++;
        return *this;
    }

    /**
     * Add a timestamp value for the current parameter
     * @throws ParamMismatchException Supplied parameter is the wrong type for this position or too many have been set
     * @return Reference to this parameter set to allow invocation chaining.
     */
    ParameterSet& addTimestamp(errType& err, int64_t val) {
        validateType(WIRE_TYPE_TIMESTAMP, false);
        m_buffer.ensureRemaining(9);
        m_buffer.putInt8(err, WIRE_TYPE_TIMESTAMP);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer.putInt64(err, val);
        if (!isOk(err)) {
            return *this;
        }
        m_currentParam++;
        return *this;
    }

    /**
     * Add an array of timestamp values for the current parameter
     * @throws ParamMismatchException Supplied parameter is the wrong type for this position or too many have been set
     * @return Reference to this parameter set to allow invocation chaining.
     */
    ParameterSet& addTimestamp(errType& err, std::vector<int64_t> vals) {
        validateType(WIRE_TYPE_TIMESTAMP, true);
        m_buffer.ensureRemaining(4 + static_cast<int32_t>(sizeof(int64_t) * vals.size()));
        m_buffer.putInt8(err, WIRE_TYPE_ARRAY);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer.putInt8(err, WIRE_TYPE_TIMESTAMP);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer.putInt16(err, static_cast<int16_t>(vals.size()));
        if (!isOk(err)) {
            return *this;
        }
        for (std::vector<int64_t>::iterator i = vals.begin(); i != vals.end(); i++) {
            m_buffer.putInt64(err, *i);
            if (!isOk(err)) {
                return *this;
            }
        }
        m_currentParam++;
        return *this;
    }

    /**
     * Add an int64_t/BIGINT value for the current parameter
     * @throws ParamMismatchException Supplied parameter is the wrong type for this position or too many have been set
     * @return Reference to this parameter set to allow invocation chaining.
     */
    ParameterSet& addInt64(errType& err, int64_t val) {
        validateType(WIRE_TYPE_BIGINT, false);
        m_buffer.ensureRemaining(9);
        m_buffer.putInt8(err, WIRE_TYPE_BIGINT);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer.putInt64(err, val);
        if (!isOk(err)) {
            return *this;
        }
        m_currentParam++;
        return *this;
    }

    /**
     * Add an array of int64_t/BIGINT values for the current parameter
     * @throws ParamMismatchException Supplied parameter is the wrong type for this position or too many have been set
     * @return Reference to this parameter set to allow invocation chaining.
     */
    ParameterSet& addInt64(errType& err, std::vector<int64_t> vals) {
        validateType(WIRE_TYPE_BIGINT, true);
        m_buffer.ensureRemaining(4 + static_cast<int32_t>(sizeof(int64_t) * vals.size()));
        m_buffer.putInt8(err,WIRE_TYPE_ARRAY);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer.putInt8(err, WIRE_TYPE_BIGINT);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer.putInt16(err, static_cast<int16_t>(vals.size()));
        if (!isOk(err)) {
            return *this;
        }
        for (std::vector<int64_t>::iterator i = vals.begin(); i != vals.end(); i++) {
            m_buffer.putInt64(err, *i);
            if (!isOk(err)) {
                return *this;
            }
        }
        m_currentParam++;
        return *this;
    }

    /**
     * Add an int32_t/INTEGER value for the current parameter
     * @throws ParamMismatchException Supplied parameter is the wrong type for this position or too many have been set
     * @return Reference to this parameter set to allow invocation chaining.
     */
    ParameterSet& addInt32(errType& err, int32_t val) {
        validateType(WIRE_TYPE_INTEGER, false);
        m_buffer.ensureRemaining(5);
        m_buffer.putInt8(err, WIRE_TYPE_INTEGER);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer.putInt32(err, val);
        if (!isOk(err)) {
            return *this;
        }
        m_currentParam++;
        return *this;
    }

    /**
     * Add an array of int32_t/INTEGER values for the current parameter
     * @throws ParamMismatchException Supplied parameter is the wrong type for this position or too many have been set
     * @return Reference to this parameter set to allow invocation chaining.
     */
    ParameterSet& addInt32(errType &err, std::vector<int32_t> vals) {
        validateType(WIRE_TYPE_INTEGER, true);
        m_buffer.ensureRemaining(4 + static_cast<int32_t>(sizeof(int32_t) * vals.size()));
        m_buffer.putInt8(err, WIRE_TYPE_ARRAY);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer.putInt8(err, WIRE_TYPE_INTEGER);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer.putInt16(err, static_cast<int16_t>(vals.size()));
        if (!isOk(err)) {
            return *this;
        }
        for (std::vector<int32_t>::iterator i = vals.begin(); i != vals.end(); i++) {
            m_buffer.putInt32(err, *i);
            if (!isOk(err)) {
                return *this;
            }
        }
        m_currentParam++;
        return *this;
    }

    /**
     * Add an int16_t/SMALLINT value for the current parameter
     * @throws ParamMismatchException Supplied parameter is the wrong type for this position or too many have been set
     * @return Reference to this parameter set to allow invocation chaining.
     */
    ParameterSet& addInt16(errType& err, int16_t val) {
        validateType(WIRE_TYPE_SMALLINT, false);
        m_buffer.ensureRemaining(3);
        m_buffer.putInt8(err, WIRE_TYPE_SMALLINT);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer.putInt16(err, val);
        if (!isOk(err)) {
            return *this;
        }
        m_currentParam++;
        return *this;
    }

    /**
     * Add an array of int16_t/SMALLINT values for the current parameter
     * @throws ParamMismatchException Supplied parameter is the wrong type for this position or too many have been set
     * @return Reference to this parameter set to allow invocation chaining.
     */
    ParameterSet& addInt16(errType& err, std::vector<int16_t> vals) {
        validateType(WIRE_TYPE_SMALLINT, true);
        m_buffer.ensureRemaining(4 + static_cast<int32_t>(sizeof(int16_t) * vals.size()));
        m_buffer.putInt8(err, WIRE_TYPE_ARRAY);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer.putInt8(err, WIRE_TYPE_SMALLINT);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer.putInt16(static_cast<int16_t>(vals.size()));
        if (!isOk(err)) {
            return *this;
        }
        for (std::vector<int16_t>::iterator i = vals.begin(); i != vals.end(); i++) {
            m_buffer.putInt16(err,*i);
            if (!isOk(err)) {
                return *this;
            }
        }
        m_currentParam++;
        return *this;
    }

    /**
     * Add an int8_t/TINYINT value for the current parameter
     * @throws ParamMismatchException Supplied parameter is the wrong type for this position or too many have been set
     * @return Reference to this parameter set to allow invocation chaining.
     */
    ParameterSet& addInt8(errType& err, int8_t val) {
        validateType(WIRE_TYPE_TINYINT, false);
        m_buffer.ensureRemaining(2);
        m_buffer.putInt8(err, WIRE_TYPE_TINYINT);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer.putInt8(err, val);
        if (!isOk(err)) {
            return *this;
        }
        m_currentParam++;
        return *this;
    }

    /**
     * Add an array of int8_t/TINYINT values for the current parameter
     * @throws ParamMismatchException Supplied parameter is the wrong type for this position or too many have been set
     * @return Reference to this parameter set to allow invocation chaining.
     */
    ParameterSet& addInt8(errType &err, std::vector<int8_t> vals) {
        validateType(WIRE_TYPE_TINYINT, true);
        m_buffer.ensureRemaining(6 + static_cast<int32_t>(sizeof(int8_t) * vals.size()));
        m_buffer.putInt8(err, WIRE_TYPE_ARRAY);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer.putInt8(err, WIRE_TYPE_TINYINT);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer.putInt32(err, static_cast<int32_t>(vals.size()));
        if (!isOk(err)) {
            return *this;
        }
        for (std::vector<int8_t>::iterator i = vals.begin(); i != vals.end(); i++) {
            m_buffer.putInt8(err, *i);
            if (!isOk(err)) {
                return *this;
            }
        }
        m_currentParam++;
        return *this;
    }

    /**
     * Add a double value for the current parameter
     * @throws ParamMismatchException Supplied parameter is the wrong type for this position or too many have been set
     * @return Reference to this parameter set to allow invocation chaining.
     */
    ParameterSet& addDouble(errType& err, double val) {
        validateType(WIRE_TYPE_FLOAT, false);
        m_buffer.ensureRemaining(9);
        m_buffer.putInt8(err, WIRE_TYPE_FLOAT);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer.putDouble(err, val);
        if (!isOk(err)) {
            return *this;
        }
        m_currentParam++;
        return *this;
    }

    /**
     * Add an array of double values for the current parameter
     * @throws ParamMismatchException Supplied parameter is the wrong type for this position or too many have been set
     * @return Reference to this parameter set to allow invocation chaining.
     */
    ParameterSet& addDouble(errType& err, std::vector<double> vals) {
        validateType(WIRE_TYPE_FLOAT, true);
        m_buffer.ensureRemaining(2 + static_cast<int32_t>(sizeof(double) * vals.size()));
        m_buffer.putInt8(err, WIRE_TYPE_ARRAY);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer.putInt8(err, WIRE_TYPE_FLOAT);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer.putInt16(err, static_cast<int16_t>(vals.size()));
        if (!isOk(err)) {
            return *this;
        }
        for (std::vector<double>::iterator i = vals.begin(); i != vals.end(); i++) {
            m_buffer.putDouble(err, *i);
            if (!isOk(err)) {
                return *this;
            }
        }
        m_currentParam++;
        return *this;
    }

    /**
     * Add null for the current parameter. The meaning of this can be tricky. This results in the SQL null
     * value used by Volt being sent across the wire so that it will represent SQL NULL if inserted into the
     * database. For numbers this the minimum representable value for the type. For strings this results in a null
     * object reference being passed to the procedure.
     * @throws ParamMismatchException Supplied parameter is the wrong type for this position or too many have been set
     * @return Reference to this parameter set to allow invocation chaining.
     */
    ParameterSet& addNull(errType& err) {
        if (m_currentParam > m_parameters.size()) {
            throw new ParamMismatchException();
        }
        m_buffer.ensureRemaining(1);
        m_buffer.putInt8(err, WIRE_TYPE_NULL);
        if (!isOk(err)) {
            return *this;
        }
        m_currentParam++;
        return *this;
    }

    /**
     * Add a string value for the current parameter
     * @throws ParamMismatchException Supplied parameter is the wrong type for this position or too many have been set
     * @return Reference to this parameter set to allow invocation chaining.
     */
    ParameterSet& addString(errType& err, std::string val) {
        validateType(WIRE_TYPE_STRING, false);
        m_buffer.ensureRemaining(5 + static_cast<int32_t>(val.size()));
        m_buffer.putInt8(err, WIRE_TYPE_STRING);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer.putString(err, val);
        if (!isOk(err)) {
            return *this;
        }
        m_currentParam++;
        return *this;
    }

    /**
     * Add an array of string values for the current parameter
     * @throws ParamMismatchException Supplied parameter is the wrong type for this position or too many have been set
     * @return Reference to this parameter set to allow invocation chaining.
     */
    ParameterSet& addString(errType& err, std::vector<std::string> vals) {
        validateType(WIRE_TYPE_STRING, true);
        int32_t totalStringLength = 0;
        for (std::vector<std::string>::iterator i = vals.begin(); i != vals.end(); i++) {
            totalStringLength += static_cast<int32_t>((*i).size());
        }
        m_buffer.ensureRemaining(4 +
                totalStringLength +
                (4 * static_cast<int32_t>(vals.size())));
        m_buffer.putInt8(err, WIRE_TYPE_ARRAY);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer.putInt8(err, WIRE_TYPE_STRING);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer.putInt16(err, static_cast<int16_t>(vals.size()));
        if (!isOk(err)) {
            return *this;
        }
        for (std::vector<std::string>::iterator i = vals.begin(); i != vals.end(); i++) {
            m_buffer.putString(err, *i);
            if (!isOk(err)) {
                return *this;
            }
        }
        m_currentParam++;
        return *this;
    }

    /**
     * Reset the parameter set so that a new set of parameters can be added. It is not necessary
     * to call this between invocations because the API will call it after the procedure this parameter
     * set is associated with is invoked.
     */
    void reset(errType& err) {
        m_buffer.clear();
        m_currentParam = 0;
        m_buffer.putInt16(err, static_cast<int16_t>(m_parameters.size()));
    }

    int32_t getSerializedSize(errType& err) {
        if (m_currentParam != m_parameters.size()) {
            setErr(err, errUninitializedParamsException);
        }
        return m_buffer.position();
    }

#ifdef SWIG
%ignore serializeTo;
#endif
    void serializeTo(errType& err, ByteBuffer *buffer) {
        if (m_currentParam != m_parameters.size()) {
            setErr(err, errUninitializedParamsException);
        }
        m_buffer.flip();
        buffer->put(err, &m_buffer);
        if (!isOk(err)) {
            return;
        }
        reset(err);
    }

private:

    ParameterSet(std::vector<Parameter> parameters) : m_parameters(parameters), m_buffer(8192), m_currentParam(0) {
        errType TODO_ERROR;
        m_buffer.putInt16(TODO_ERROR, static_cast<int16_t>(m_parameters.size()));
    }

    void validateType(WireType type, bool isArray) {
        if (m_currentParam >= m_parameters.size() ||
                m_parameters[m_currentParam].m_type != type ||
                m_parameters[m_currentParam].m_array != isArray) {
            throw ParamMismatchException();
        }
    }

    std::vector<Parameter> m_parameters;
    ScopedByteBuffer m_buffer;
    uint32_t m_currentParam;
};
}
#endif /* VOLTDB_PARAMETERSET_HPP_ */
