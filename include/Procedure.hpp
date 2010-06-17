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

#ifndef PROCEDURE_HPP_
#define PROCEDURE_HPP_
#include <vector>
#include <string>
#include "ParameterSet.hpp"
#include "ByteBuffer.hpp"

namespace voltdb {
class Procedure {
public:
    Procedure(std::string name, std::vector<Parameter> parameters) : m_name(name), m_params(parameters) {}

    /**
     * Retrieve the parameter set and set the client data for the next invocation
     */
    ParameterSet* params() {
        m_params.reset();
        return &m_params;
    }

    int32_t getSerializedSize() {
        return 5 + //length prefix
               4 + static_cast<int32_t>(m_name.size()) + // proc name
                m_params.getSerializedSize() + //parameters
                + 8; //client data
    }

    void serializeTo(ByteBuffer *buffer, int64_t clientData) {
        buffer->position(4);
        buffer->putInt8(0);
        buffer->putString(m_name);
        buffer->putInt64(clientData);
        m_params.serializeTo(buffer);
        buffer->flip();
        buffer->putInt32( 0, buffer->limit() - 4);
    }
private:
    std::string m_name;
    ParameterSet m_params;
};
}

#endif /* PROCEDURE_HPP_ */
