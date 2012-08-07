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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef VOLTDB_BYTEBUFFER_H
#define VOLTDB_BYTEBUFFER_H

#include <stdint.h>
#include <arpa/inet.h>
#include <boost/shared_array.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_array.hpp>
#include <boost/scoped_ptr.hpp>
#include <vector>
#include <string>
#include <cstring>
#include "Exception.hpp"

namespace voltdb {


#ifdef __DARWIN_OSSwapInt64 // for darwin/macosx

#define htonll(x) __DARWIN_OSSwapInt64(x)
#define ntohll(x) __DARWIN_OSSwapInt64(x)

#else // unix in general

//#undef htons
//#undef ntohs
//#define htons(x) static_cast<uint16_t>((((x) >> 8) & 0xff) | (((x) & 0xff) << 8))
//#define ntohs(x) static_cast<uint16_t>((((x) >> 8) & 0xff) | (((x) & 0xff) << 8))

#ifdef __bswap_64 // recent linux

#define htonll(x) static_cast<uint64_t>(__bswap_constant_64(x))
#define ntohll(x) static_cast<uint64_t>(__bswap_constant_64(x))

#else // unix in general again

#define htonll(x) (((int64_t)(ntohl((int32_t)((x << 32) >> 32))) << 32) | (uint32_t)ntohl(((int32_t)(x >> 32))))
#define ntohll(x) (((int64_t)(ntohl((int32_t)((x << 32) >> 32))) << 32) | (uint32_t)ntohl(((int32_t)(x >> 32))))

#endif // __bswap_64

#endif // unix or mac

class ByteBufferTest;
class ByteBuffer {
    friend class ByteBufferTest;
private:
    int32_t checkGetPutIndex(errType& err, int32_t length) {
        if (m_limit - m_position < length || length < 0) {
            setErr(err, errOverflowUnderflowException);
            return -1;
        }
        int32_t position = m_position;
        m_position += length;
        return position;
    }

    int32_t checkIndex(errType& err, int32_t index, int32_t length) {
        if ((index < 0) || (length > m_limit - index) || length < 0) {
            setErr(err, errIndexOutOfBoundsException);
            return -1;
        }
        return index;
    }
public:
    ByteBuffer& flip() {
        m_limit = m_position;
        m_position = 0;
        return *this;
    }
    ByteBuffer& clear() {
        m_limit = m_capacity;
        m_position = 0;
        return *this;
    }

    void get(errType& err, char *storage, int32_t length) {
        int32_t idx = checkGetPutIndex(err, length);
        if (!isOk(err)) {
            return;
        }
        ::memcpy(storage, &m_buffer[idx], static_cast<uint32_t>(length));
    }
    void get(errType& err, int32_t index, char *storage, int32_t length) {
        int32_t idx = checkIndex(err, index, length);
        if (!isOk(err)) {
            return;
        }
        ::memcpy(storage, &m_buffer[idx], static_cast<uint32_t>(length));
    }
    ByteBuffer& put(errType& err, const char *storage, int32_t length) {
        int32_t idx = checkGetPutIndex(err, length);
        if (!isOk(err)) {
            return *this;
        }
        ::memcpy(&m_buffer[idx], storage, static_cast<uint32_t>(length));
        return *this;
    }
    ByteBuffer& put(errType& err, int32_t index, const char *storage, int32_t length) {
        int32_t idx = checkIndex(err, index, length);
        if (!isOk(err)) {
            return *this;
        }
        ::memcpy(&m_buffer[idx], storage, static_cast<uint32_t>(length));
        return *this;
    }

    ByteBuffer& put(errType& err, ByteBuffer *other) {
        int32_t oremaining = other->remaining();
        if (oremaining == 0) {
            return *this;
        }
        int32_t idx = checkGetPutIndex(err, oremaining);
        if (!isOk(err)) {
            return *this;
        }
        ::memcpy(&m_buffer[idx],
                other->getByReference(err, oremaining),
                static_cast<uint32_t>(oremaining));
        return *this;
    }

    int8_t getInt8(errType& err) {
        int32_t idx = checkGetPutIndex(err, 1);
        if (!isOk(err)) {
            return 0x0;
        }
        return m_buffer[idx];
    }
    int8_t getInt8(errType& err, int32_t index) {
        int32_t idx = checkIndex(err, index, 1);
        if (!isOk(err)) {
            return 0x0;
        }
        return m_buffer[idx];
    }
    ByteBuffer& putInt8(errType& err, int8_t value) {
        int32_t idx = checkGetPutIndex(err, 1);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer[idx] = value;
        return *this;
    }
    ByteBuffer& putInt8(errType& err, int32_t index, int8_t value) {
        int32_t idx = checkIndex(err, index, 1);
        if (!isOk(err)) {
            return *this;
        }
        m_buffer[idx] = value;
        return *this;
    }

    int16_t getInt16(errType& err) {
        int16_t value;
        int32_t idx = checkGetPutIndex(err, 2);
        if (!isOk(err)) {
            return 0x0;
        }
        ::memcpy( &value, &m_buffer[idx], 2);
        return static_cast<int16_t>(ntohs(value));
    }
    int16_t getInt16(errType& err, int32_t index) {
        int16_t value;
        int32_t idx = checkIndex(err, index, 2);
        if (!isOk(err)) {
            return 0x0;
        }
        ::memcpy( &value, &m_buffer[idx], 2);
        return static_cast<int16_t>(ntohs(value));
    }
    ByteBuffer& putInt16(errType& err, int16_t value) {
        int32_t idx = checkGetPutIndex(err, 2);
        if (!isOk(err)) {
            return *this;
        }
        *reinterpret_cast<uint16_t*>(&m_buffer[idx]) = htons(value);
        return *this;
    }
    ByteBuffer& putInt16(errType& err, int32_t index, int16_t value) {
        int32_t idx = checkIndex(err, index, 2);
        if (!isOk(err)) {
            return *this;
        }
        *reinterpret_cast<uint16_t*>(&m_buffer[idx]) = htons(value);
        return *this;
    }

    int32_t getInt32(errType& err) {
        uint32_t value;
        int32_t idx = checkGetPutIndex(err, 4);
        if (!isOk(err)) {
            return 0;
        }
        ::memcpy( &value, &m_buffer[idx], 4);
        return static_cast<int32_t>(ntohl(value));
    }
    int32_t getInt32(errType& err, int32_t index) {
        uint32_t value;
        int32_t idx = checkIndex(err, index, 4);
        if (!isOk(err)) {
            return 0;
        }
        ::memcpy( &value, &m_buffer[idx], 4);
        return static_cast<int32_t>(ntohl(value));
    }
    ByteBuffer& putInt32(errType& err, int32_t value) {
        int32_t idx = checkGetPutIndex(err, 4);
        if (!isOk(err)) {
            return *this;
        }
        *reinterpret_cast<uint32_t*>(&m_buffer[idx]) = htonl(static_cast<uint32_t>(value));
        return *this;
    }
    ByteBuffer& putInt32(errType& err, int32_t index, int32_t value) {
        int32_t idx = checkIndex(err, index, 4);
        if (!isOk(err)) {
            return *this;
        }
        *reinterpret_cast<uint32_t*>(&m_buffer[idx]) = htonl(static_cast<uint32_t>(value));
        return *this;
    }

    int64_t getInt64(errType& err) {
        uint64_t value;
        int32_t idx = checkGetPutIndex(err, 8);
        if (!isOk(err)) {
            return 0L;
        }
        ::memcpy( &value, &m_buffer[idx], 8);
        return static_cast<int64_t>(ntohll(value));
    }
    int64_t getInt64(errType& err, int32_t index) {
        uint64_t value;
        int32_t idx = checkIndex(err, index, 8);
        if (!isOk(err)) {
            return 0L;
        }
        ::memcpy( &value, &m_buffer[idx], 8);
        return static_cast<int64_t>(ntohll(value));
    }
    ByteBuffer& putInt64(errType& err, int64_t value) {
        int32_t idx = checkGetPutIndex(err, 8);
        if (!isOk(err)) {
            return *this;
        }
        *reinterpret_cast<uint64_t*>(&m_buffer[idx]) = htonll(static_cast<uint64_t>(value));
        return *this;
    }
    ByteBuffer& putInt64(errType& err, int32_t index, int64_t value) {
        int32_t idx = checkIndex(err, index, 8);
        if (!isOk(err)) {
            return *this;
        }
        *reinterpret_cast<uint64_t*>(&m_buffer[idx]) = htonll(static_cast<uint64_t>(value));
        return *this;
    }

    double getDouble(errType& err) {
        uint64_t value;
        int32_t idx = checkGetPutIndex(err, 8);
        if (!isOk(err)) {
            return 0.0;
        }
        ::memcpy( &value, &m_buffer[idx], 8);
        value = ntohll(value);
        double retval;
        ::memcpy( &retval, &value, 8);
        return retval;
    }
    double getDouble(errType& err, int32_t index) {
        uint64_t value;
        int32_t idx = checkIndex(err, index, 8);
        if (!isOk(err)) {
            return 0.0;
        }
        ::memcpy( &value, &m_buffer[idx], 8);
        value = ntohll(value);
        double retval;
        ::memcpy( &retval, &value, 8);
        return retval;
    }
    ByteBuffer& putDouble(errType& err, double value) {
        uint64_t newval;
        int32_t idx = checkGetPutIndex(err, 8);
        if (!isOk(err)) {
            return *this;
        }
        ::memcpy(&newval, &value, 8);
        newval = htonll(newval);
        *reinterpret_cast<uint64_t*>(&m_buffer[idx]) = newval;
        return *this;
    }
    ByteBuffer& putDouble(errType& err, int32_t index, double value) {
        uint64_t newval;
        ::memcpy(&newval, &value, 8);
        newval = htonll(newval);
        int32_t idx = checkIndex(err, index, 8);
        if (!isOk(err)) {
            return *this;
        }
        *reinterpret_cast<uint64_t*>(&m_buffer[idx]) = newval;
        return *this;
    }

    std::string getString(errType& err, bool &wasNull) {
        int32_t length = getInt32(err);
        if (!isOk(err)) {
            return std::string();
        }
        if (length == -1) {
            wasNull = true;
            return std::string();
        }
        char *data = getByReference(err, length);
        if (!isOk(err)) {
            return std::string();
        }
        return std::string(data, static_cast<uint32_t>(length));
    }
    std::string getString(errType& err, int32_t index, bool &wasNull) {
        int32_t length = getInt32(err, index);
        if (!isOk(err)) {
            return std::string();
        }
        if (length == -1) {
            wasNull = true;
            return std::string();
        }
        char *data = getByReference(err, index + 4, length);
        if (!isOk(err)) {
            return std::string();
        }
        return std::string(data, static_cast<uint32_t>(length));
    }
    ByteBuffer& putString(errType& err, std::string value) {
        int32_t size = static_cast<int32_t>(value.size());
        putInt32(err, size);
        if (!isOk(err)) {
            return *this;
        }
        put(err, value.data(), size);
        if (!isOk(err)) {
            return *this;
        }
        return *this;
    }

    bool getBytes(errType& err, bool &wasNull, int32_t bufsize, uint8_t *out_value, int32_t *out_len)
    {
        int32_t length = getInt32(err);
        if (!isOk(err)) {
            return false;
        }
        *out_len = length;
        if (!out_value)
            return false;
        if (length == -1) {
            wasNull = true;
            return true;
        }
        if (length > bufsize)
            return false;
        char *data = getByReference(err, length);
        if (!isOk(err)) {
            return false;
        }
        memcpy(out_value, data, length);
        return true;
    }
    bool getBytes(errType& err, int32_t index, bool &wasNull, const int32_t bufsize, uint8_t *out_value, int32_t *out_len)
    {
        int32_t length = getInt32(err, index);
        if (!isOk(err)) {
            return false;
        }
        *out_len = length;
        if (!out_value)
            return false;
        if (length == -1) {
            wasNull = true;
            return true;
        }
        if (length > bufsize)
            return false;
        char *data = getByReference(err, index + 4, length);
        if (!isOk(err)) {
            return false;
        }
        memcpy(out_value, data, length);
        return true;
    }
    ByteBuffer& putBytes(errType& err, const int32_t bufsize, const uint8_t *in_value)
    {
        assert(in_value);
        putInt32(err, bufsize);
        if (!isOk(err)) {
            return *this;
        }
        put(err, (const char*)in_value, bufsize);
        return *this;
    }
    ByteBuffer& putBytes(errType& err, int32_t index, const int32_t bufsize, const uint8_t *in_value)
    {
        assert(in_value);
        putInt32(err, index, bufsize);
        if (!isOk(err)) {
            return *this;
        }
        put(err, index + 4, (const char*)in_value, bufsize);
        if (!isOk(err)) {
            return *this;
        }
        return *this;
    }
    ByteBuffer& putString(errType& err, int32_t index, std::string value) {
        int32_t size = static_cast<int32_t>(value.size());
        putInt32(err, index, size);
        if (!isOk(err)) {
            return *this;
        }
        put(err, index + 4, value.data(), size);
        if (!isOk(err)) {
            return *this;
        }
        return *this;
    }

    int32_t position() {
        return m_position;
    }
    ByteBuffer& position(errType& err, int32_t position) {
        int32_t idx = checkIndex(err, position, 0);
        if (!isOk(err)) {
            return *this;
        }
        m_position = idx;
        return *this;
    }

    int32_t remaining() {
        return m_limit - m_position;
    }
    bool hasRemaining() {
        return m_position < m_limit;
    }

    int32_t limit() {
        return m_limit;
    }

    ByteBuffer& limit(errType& err, int32_t newLimit) {
        if (newLimit > m_capacity || newLimit < 0) {
            setErr(err, errIndexOutOfBoundsException);
            return *this;
        }
        m_limit = newLimit;
        return *this;
    }

    char* bytes() {
        return m_buffer;
    }

    ByteBuffer slice() {
        ByteBuffer retval(&m_buffer[m_position], m_limit - m_position);
        m_position = m_limit;
        return retval;
    }

    virtual bool isExpandable() {
        return false;
    }

    /**
     * Create a byte buffer backed by the provided storage. Does not handle memory ownership
     */
    ByteBuffer(char *buffer, int32_t capacity) :
        m_buffer(buffer), m_position(0), m_capacity(capacity), m_limit(capacity) {
        if (buffer == NULL) {
            m_buffer = NULL;
            m_position = 0;
            m_capacity = 0;
            m_limit = 0;
        }
    }

    ByteBuffer(const ByteBuffer &other) :
        m_buffer(other.m_buffer), m_position(other.m_position), m_capacity(other.m_capacity), m_limit(other.m_limit) {
    }

    virtual ~ByteBuffer() {}

    int32_t capacity() {
        return m_capacity;
    }
private:
    ByteBuffer& operator = (const ByteBuffer& other) {
        m_buffer = other.m_buffer;
        m_position = other.m_position;
        m_capacity = other.m_capacity;
        m_limit = other.m_limit;
        return *this;
    }
    char * getByReference(errType& err, int32_t length) {
        int32_t idx = checkGetPutIndex(err, length);
        if (!isOk(err)) {
            return NULL;
        }
        return &m_buffer[idx];
    }
    char * getByReference(errType& err, int32_t index, int32_t length) {
        int32_t idx = checkIndex(err, index, length);
        if (!isOk(err)) {
            return NULL;
        }
        return &m_buffer[idx];
    }
protected:
    ByteBuffer() : m_buffer(NULL), m_position(0), m_capacity(0), m_limit(0) {};
    char *  m_buffer;
    int32_t m_position;
    int32_t m_capacity;
    int32_t m_limit;
};

class ExpandableByteBuffer : public ByteBuffer {
public:
    void ensureRemaining(int32_t amount)  {
        if (remaining() < amount) {
            ensureCapacity(position() + amount);
        }
    }
    void ensureRemainingExact(int32_t amount)  {
        if (remaining() < amount) {
            ensureCapacityExact(position() + amount);
        }
    }
    void ensureCapacity(int32_t capacity)  {
        if (m_capacity < capacity) {
            int32_t newCapacity = m_capacity;
            while (newCapacity < capacity) {
                newCapacity *= 2;
            }
            char *newBuffer = new char[newCapacity];
            ::memcpy(newBuffer, m_buffer, static_cast<uint32_t>(m_position));
            m_buffer = newBuffer;
            resetRef(newBuffer);
            m_capacity = newCapacity;
            m_limit = newCapacity;
        }
    }

    void ensureCapacityExact(int32_t capacity)  {
        if (m_capacity < capacity) {
            char *newBuffer = new char[capacity];
            ::memcpy(newBuffer, m_buffer, static_cast<uint32_t>(m_position));
            m_buffer = newBuffer;
            resetRef(newBuffer);
            m_capacity = capacity;
            m_limit = capacity;
        }
    }

    bool isExpandable() {
        return true;
    }

    virtual ~ExpandableByteBuffer() {}
protected:
    ExpandableByteBuffer(const ExpandableByteBuffer &other) : ByteBuffer(other) {
    }
    ExpandableByteBuffer() : ByteBuffer() {}
    ExpandableByteBuffer(char *data, int32_t length) : ByteBuffer(data, length) {}
    virtual void resetRef(char *data) = 0;
private:
};

class SharedByteBuffer : public ExpandableByteBuffer {
public:
    SharedByteBuffer(const SharedByteBuffer &other) : ExpandableByteBuffer(other), m_ref(other.m_ref) {}

    SharedByteBuffer& operator = (const SharedByteBuffer& other) {
        m_ref = other.m_ref;
        m_buffer = other.m_buffer;
        m_position = other.m_position;
        m_limit = other.m_limit;
        return *this;
    }

    SharedByteBuffer() : ExpandableByteBuffer() {};

    SharedByteBuffer slice() {
        SharedByteBuffer retval(m_ref, &m_buffer[m_position], m_limit - m_position);
        m_position = m_limit;
        return retval;
    }

    SharedByteBuffer(char *data, int32_t length) : ExpandableByteBuffer(data, length), m_ref(data) {}
    SharedByteBuffer(boost::shared_array<char> data, int32_t length) : ExpandableByteBuffer(data.get(), length), m_ref(data) {}

protected:
    void resetRef(char *data)  {
        m_ref.reset(data);
    }
private:
    SharedByteBuffer(boost::shared_array<char> ref, char *data, int32_t length) : ExpandableByteBuffer(data, length), m_ref(ref) {}
    boost::shared_array<char> m_ref;
};

class ScopedByteBuffer : public ExpandableByteBuffer {
public:
//    virtual ByteBuffer* duplicate() {
//        char *copy = new char[m_capacity];
//        ::memcpy(copy, m_buffer, m_capacity);
//        return new ScopedByteBuffer(copy, m_capacity);
//    }
    ScopedByteBuffer(int32_t capacity) : ExpandableByteBuffer(new char[capacity], capacity), m_ref(m_buffer) {

    }
    ScopedByteBuffer(char *data, int32_t length) : ExpandableByteBuffer(data, length), m_ref(data) {}

//    static void copyAndWrap(ScopedByteBuffer &out, char *source, int32_t length) {
//        assert(length > 0);
//        char *copy = new char[length];
//        ::memcpy( copy, source, length);
//
//        return out(copy, length);
//    }
protected:
    void resetRef(char *data)  {
        m_ref.reset(data);
    }
private:
    ScopedByteBuffer& operator = (const ScopedByteBuffer& other) {
        assert(other.m_buffer != NULL);
        return *this;
    }
    ScopedByteBuffer(const ScopedByteBuffer &other) : ExpandableByteBuffer(other) {
    }
    ScopedByteBuffer() : ExpandableByteBuffer(), m_ref()  {};
    boost::scoped_array<char> m_ref;
};

}
#endif
