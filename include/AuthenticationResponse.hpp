/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

#ifndef VOLTDB_AUTHENTICATIONRESPONSE_HPP_
#define VOLTDB_AUTHENTICATIONRESPONSE_HPP_
#include "ByteBuffer.hpp"
namespace voltdb {
class AuthenticationResponse {
public:
    AuthenticationResponse() {
        m_err = errOk;
    }

    AuthenticationResponse(ByteBuffer &buf) : m_err(errOk) {
        char version = buf.getInt8(m_err);
        if (!isOk(m_err)) {
            return;
        }

        assert(version == 0);
        m_resultCode = buf.getInt8(m_err);
        if (!isOk(m_err)) {
            return;
        }
        if (m_resultCode != 0) {
            return;
        }
        m_hostId = buf.getInt32(m_err);
        if (!isOk(m_err)) {
            return;
        }
        m_connectionId = buf.getInt64(m_err);
        if (!isOk(m_err)) {
            return;
        }
        m_clusterStartTime = buf.getInt64(m_err);
        if (!isOk(m_err)) {
            return;
        }
        if (!isOk(m_err)) {
            return;
        }
        m_leaderAddress = buf.getInt32(m_err);
        bool wasNull = false;
        m_buildString = buf.getString(m_err, wasNull);
        if (!isOk(m_err)) {
            return;
        }
        assert(!wasNull);
    }

    bool success() {
        return isOk(getErr()) && (m_resultCode == 0);
    }

    errType getErr() { return m_err; }
    int32_t hostId() { return m_hostId; }
    int32_t connectionId() { return m_connectionId; }
    int64_t clusterStartTime() { return m_clusterStartTime; }
    int32_t leaderAddress() { return m_leaderAddress; }
    std::string buildString() { return m_buildString; }

private:
    errType m_err;
    int8_t m_resultCode;
    int32_t m_hostId;
    int64_t m_connectionId;
    int64_t m_clusterStartTime;
    int32_t m_leaderAddress;
    std::string m_buildString;
};
}

#endif /* VOLTDB_AUTHENTICATIONRESPONSE_HPP_ */
