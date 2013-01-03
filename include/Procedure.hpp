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

#ifndef VOLTDB_PROCEDURE_HPP_
#define VOLTDB_PROCEDURE_HPP_
#include <vector>
#include <string>
#include "ParameterSet.hpp"
#include "ByteBuffer.hpp"

namespace voltdb {

/*
 * Description of a stored procedure and its parameters that must be provided to the API
 * in order to invoke a stored procedure
 */
class Procedure {
public:
    /*
     * Construct a Procedure with the specified name and specified signature (parameters)
     */
    Procedure(std::string name, std::vector<Parameter> parameters) : m_name(name), m_params(parameters) {}

    /**
     * Retrieve the parameter set associated with the procedure so that the parameters can be set
     * with actual values. The pointer to the parameter set can be retained
     * as long as the Procedure itself is still valid, and can be reused after each invocation.
     */
    ParameterSet* params() {
        // assume reset() doesn't fail.
        errType TODO_ERROR = errOk;
        m_params.reset(TODO_ERROR);
        return &m_params;
    }

    // TODO_ERROR
    int32_t getSerializedSize(errType& err) {
        return 5 + //length prefix
               4 + static_cast<int32_t>(m_name.size()) + // proc name
                m_params.getSerializedSize(err) + //parameters
                + 8; //client data
    }

#ifdef SWIG
%ignore serializeTo;
#endif
    void serializeTo(errType& err, ByteBuffer *buffer, int64_t clientData) {
        buffer->position(err, 4);
        if (!isOk(err)) {
            return;
        }
        buffer->putInt8(err, 0);
        if (!isOk(err)) {
            return;
        }
        buffer->putString(err, m_name);
        if (!isOk(err)) {
            return;
        }
        buffer->putInt64(err, clientData);
        if (!isOk(err)) {
            return;
        }
        m_params.serializeTo(err, buffer);
        if (!isOk(err)) {
            return;
        }
        buffer->flip();
        buffer->putInt32(err, 0, buffer->limit() - 4);
        if (!isOk(err)) {
            return;
        }
    }
private:
    std::string m_name;
    ParameterSet m_params;
};
}

#endif /* VOLTDB_PROCEDURE_HPP_ */
