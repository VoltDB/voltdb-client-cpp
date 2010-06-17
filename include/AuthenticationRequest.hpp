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

#ifndef VOLTDB_AUTHENTICATIONMESSAGE_HPP_
#define VOLTDB_AUTHENTICATIONMESSAGE_HPP_
#include "ByteBuffer.hpp"
#include <openssl/sha.h>
namespace voltdb {
class AuthenticationRequest {
public:
    AuthenticationRequest(std::string username, std::string service, std::string password) :
        m_username(username), m_service(service), m_password(password) {}
    int32_t getSerializedSize() {
        return 8 + //String length prefixes
        20 + //SHA-1 hash of PW
        m_username.size() +
        m_service.size()
        + 4 //length prefix
        + 1; //version number
    }
    void serializeTo(ByteBuffer &buffer) {
        buffer.position(4);
        buffer.putInt8(0);
        buffer.putString(m_service);
        buffer.putString(m_username);
        unsigned char hashBytes[20];
        SHA1( reinterpret_cast<const unsigned char*>(m_password.data()), m_password.size(), hashBytes);
        buffer.put(reinterpret_cast<char*>(hashBytes), 20);
        buffer.flip();
        buffer.putInt32(0, buffer.limit() - 4);
    }
private:
    std::string m_username;
    std::string m_service;
    std::string m_password;
};
}

#endif /* VOLTDB_AUTHENTICATIONMESSAGE_HPP_ */
