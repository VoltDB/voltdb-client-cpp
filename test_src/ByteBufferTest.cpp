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
#include <cppunit/TestCase.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestFixture.h>
#include "Exception.hpp"
#include "ByteBuffer.hpp"
#include <boost/scoped_ptr.hpp>

namespace voltdb {

class ByteBufferTest : public CppUnit::TestFixture {
CPPUNIT_TEST_SUITE( ByteBufferTest );
CPPUNIT_TEST_EXCEPTION( checkNullBuffer, voltdb::NullPointerException );
CPPUNIT_TEST( checkGetPutIndexValid );
CPPUNIT_TEST( checkGetPutIndexNegative);
CPPUNIT_TEST( checkGetPutIndexOver);
CPPUNIT_TEST( checkIndexNegativeIndex );
CPPUNIT_TEST( checkIndexOverIndex );
CPPUNIT_TEST( checkIndexNegativeLength );
CPPUNIT_TEST( checkIndexOverLength );
CPPUNIT_TEST( checkIndexValid );
CPPUNIT_TEST( testFlip );
CPPUNIT_TEST( testClear );
CPPUNIT_TEST( testGetCharsPosition );
CPPUNIT_TEST( testGetCharsIndex );
CPPUNIT_TEST( testPutCharsPosition );
CPPUNIT_TEST( testPutCharsIndex );
CPPUNIT_TEST( testPutBuffer );
CPPUNIT_TEST( testInt8 );
CPPUNIT_TEST( testInt16 );
CPPUNIT_TEST( testInt32 );
CPPUNIT_TEST( testInt64 );
CPPUNIT_TEST( testDouble );
CPPUNIT_TEST( testString );
CPPUNIT_TEST( testPositionNegative );
CPPUNIT_TEST( testPositionOver );
CPPUNIT_TEST( testRemainingAndHasRemaining );
CPPUNIT_TEST( testLimitNegative );
CPPUNIT_TEST( testLimitOver );
CPPUNIT_TEST( testSlice );
CPPUNIT_TEST( testIsExpandable );
CPPUNIT_TEST( testCopyConstruction );
CPPUNIT_TEST( testSharedAndScopedByteBuffer );
CPPUNIT_TEST_SUITE_END();

public:

    void checkNullBuffer() {
        ByteBuffer b(NULL, 0);
    }

    void checkGetPutIndexValid() {
        ByteBuffer b( reinterpret_cast<char*>(1), 0 );
        errType err = errOk;
        b.checkGetPutIndex(err, 0);
        CPPUNIT_ASSERT(err == errOk);
    }

    void checkGetPutIndexNegative() {
        ByteBuffer b( reinterpret_cast<char*>(1), 0 );
        errType err = errOk;
        b.checkGetPutIndex(err, -1);
        CPPUNIT_ASSERT(err == errOverflowUnderflowException);
    }

    void checkGetPutIndexOver() {
        ByteBuffer b( reinterpret_cast<char*>(1), 0);
        errType err = errOk;
        b.checkGetPutIndex(err, 1);
        CPPUNIT_ASSERT(err == errOverflowUnderflowException);
    }

    void checkIndexNegativeIndex() {
        ByteBuffer b( reinterpret_cast<char*>(1), 0);
        errType err = errOk;
        b.checkIndex(err, -1, 0);
        CPPUNIT_ASSERT(err == errIndexOutOfBoundsException);
    }

    void checkIndexNegativeLength() {
        ByteBuffer b( reinterpret_cast<char*>(1), 0);
        errType err = errOk;
        b.checkIndex(err, 0, -1);
        CPPUNIT_ASSERT(err == errIndexOutOfBoundsException);
    }

    void checkIndexOverIndex() {
        ByteBuffer b( reinterpret_cast<char*>(1), 0);
        errType err = errOk;
        b.checkIndex(err, 1, 0);
        CPPUNIT_ASSERT(err == errIndexOutOfBoundsException);
    }

    void checkIndexOverLength() {
        ByteBuffer b( reinterpret_cast<char*>(1), 0);
        errType err = errOk;
        b.checkIndex(err, 0, 1);
        CPPUNIT_ASSERT(err == errIndexOutOfBoundsException);
    }

    void checkIndexValid() {
        ByteBuffer b( reinterpret_cast<char*>(1), 0);
        errType err = errOk;
        b.checkIndex(err, 0, 0);
        CPPUNIT_ASSERT(err == errOk);
    }

    void testFlip() {
        ByteBuffer b( reinterpret_cast<char*>(1), 32);
        errType err = errOk;
        b.position(err, 16);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.position() == 16);
        CPPUNIT_ASSERT(b.limit() == 32);
        b.flip();
        CPPUNIT_ASSERT(b.position() == 0);
        CPPUNIT_ASSERT(b.limit() == 16);
        b.flip();
        CPPUNIT_ASSERT(b.position() == 0);
        CPPUNIT_ASSERT(b.limit() == 0);
    }

    void testClear() {
        ByteBuffer b( reinterpret_cast<char*>(1), 32);
        errType err = errOk;
        b.position(err, 16);
        CPPUNIT_ASSERT(err == errOk);
        b.limit(err, 16);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.position() == 16);
        CPPUNIT_ASSERT(b.limit() == 16);
        b.clear();
        CPPUNIT_ASSERT(b.position() == 0);
        CPPUNIT_ASSERT(b.limit() == 32);
        b.clear();
        CPPUNIT_ASSERT(b.position() == 0);
        CPPUNIT_ASSERT(b.limit() == 32);
    }

    void testGetCharsPosition() {
        char source[] = { 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd' };
        char dest[4];
        errType err = errOk;
        ByteBuffer b( source, 11);
        b.get(err, dest, 4);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.position() == 4);
        CPPUNIT_ASSERT(b.remaining() == 7);
        CPPUNIT_ASSERT(dest[0] == 'h');
        CPPUNIT_ASSERT(dest[1] == 'e');
        CPPUNIT_ASSERT(dest[2] == 'l');
        CPPUNIT_ASSERT(dest[3] == 'l');
    }

    void testGetCharsIndex() {
        char source[] = { 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd' };
        char dest[4];
        errType err = errOk;
        ByteBuffer b( source, 11);
        b.get(err, 4, dest, 4);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.position() == 0);
        CPPUNIT_ASSERT(b.remaining() == 11);
        CPPUNIT_ASSERT(dest[0] == 'o');
        CPPUNIT_ASSERT(dest[1] == ' ');
        CPPUNIT_ASSERT(dest[2] == 'w');
        CPPUNIT_ASSERT(dest[3] == 'o');
    }

    void testPutCharsPosition() {
        char source[] = { 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd' };
        char dest[64];
        errType err = errOk;
        ByteBuffer b( dest, 64);
        b.put(err, source, 11);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.position() == 11);
        CPPUNIT_ASSERT(b.remaining() == 53);
        CPPUNIT_ASSERT(dest[0] == 'h');
        CPPUNIT_ASSERT(dest[1] == 'e');
        CPPUNIT_ASSERT(dest[2] == 'l');
        CPPUNIT_ASSERT(dest[3] == 'l');
        CPPUNIT_ASSERT(dest[4] == 'o');
        CPPUNIT_ASSERT(dest[5] == ' ');
        CPPUNIT_ASSERT(dest[6] == 'w');
        CPPUNIT_ASSERT(dest[7] == 'o');
        CPPUNIT_ASSERT(dest[8] == 'r');
        CPPUNIT_ASSERT(dest[9] == 'l');
        CPPUNIT_ASSERT(dest[10] == 'd');
    }

    void testPutCharsIndex() {
        char source[] = { 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd' };
        char dest[64];
        errType err = errOk;
        ByteBuffer b( dest, 64);
        b.put(err, 5, source, 11);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.position() == 0);
        CPPUNIT_ASSERT(b.remaining() == 64);
        CPPUNIT_ASSERT(dest[5] == 'h');
        CPPUNIT_ASSERT(dest[6] == 'e');
        CPPUNIT_ASSERT(dest[7] == 'l');
        CPPUNIT_ASSERT(dest[8] == 'l');
        CPPUNIT_ASSERT(dest[9] == 'o');
        CPPUNIT_ASSERT(dest[10] == ' ');
        CPPUNIT_ASSERT(dest[11] == 'w');
        CPPUNIT_ASSERT(dest[12] == 'o');
        CPPUNIT_ASSERT(dest[13] == 'r');
        CPPUNIT_ASSERT(dest[14] == 'l');
        CPPUNIT_ASSERT(dest[15] == 'd');
    }

    void testPutBuffer() {
        ByteBuffer empty(reinterpret_cast<char*>(1), 0);
        char source[] = { 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd' };
        char dest[64];
        errType err = errOk;
        ByteBuffer b( dest, 64);
        b.put(err, &empty);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.position() == 0);
        CPPUNIT_ASSERT(b.remaining() == 64);
        ByteBuffer data( source, 11);
        b.put(err, &data );
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.position() == 11);
        CPPUNIT_ASSERT(b.remaining() == 53);
        CPPUNIT_ASSERT(dest[0] == 'h');
        CPPUNIT_ASSERT(dest[1] == 'e');
        CPPUNIT_ASSERT(dest[2] == 'l');
        CPPUNIT_ASSERT(dest[3] == 'l');
        CPPUNIT_ASSERT(dest[4] == 'o');
        CPPUNIT_ASSERT(dest[5] == ' ');
        CPPUNIT_ASSERT(dest[6] == 'w');
        CPPUNIT_ASSERT(dest[7] == 'o');
        CPPUNIT_ASSERT(dest[8] == 'r');
        CPPUNIT_ASSERT(dest[9] == 'l');
        CPPUNIT_ASSERT(dest[10] == 'd');
    }

    void testInt8() {
        char storage[2];
        errType err = errOk;
        ByteBuffer b( storage, 2);
        b.putInt8(err, 0, 'a');
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.remaining() == 2);
        CPPUNIT_ASSERT(b.position() == 0);
        CPPUNIT_ASSERT(b.getInt8(err, 0) == 'a');
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.remaining() == 2);
        CPPUNIT_ASSERT(b.position() == 0);
        b.putInt8(err, 'b').flip();
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.getInt8(err) == 'b');
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.remaining() == 0);
        CPPUNIT_ASSERT(b.position() == 1);
    }

    void testInt16() {
        char storage[2];
        errType err = errOk;
        ByteBuffer b( storage, 2);
        b.putInt16(err, 0, 495);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.remaining() == 2);
        CPPUNIT_ASSERT(b.position() == 0);
        CPPUNIT_ASSERT(b.getInt16(err, 0) == 495);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.remaining() == 2);
        CPPUNIT_ASSERT(b.position() == 0);
        b.putInt16(err, 500).flip();
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.getInt16(err) == 500);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.remaining() == 0);
        CPPUNIT_ASSERT(b.position() == 2);
    }

    void testInt32() {
        char storage[4];
        errType err = errOk;
        ByteBuffer b( storage, 4);
        b.putInt32(err, 0, 64000);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.remaining() == 4);
        CPPUNIT_ASSERT(b.position() == 0);
        CPPUNIT_ASSERT(b.getInt32(err, 0) == 64000);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.remaining() == 4);
        CPPUNIT_ASSERT(b.position() == 0);
        b.putInt32(err, 64001).flip();
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.getInt32(err) == 64001);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.remaining() == 0);
        CPPUNIT_ASSERT(b.position() == 4);
    }

    void testInt64() {
        char storage[8];
        errType err = errOk;
        ByteBuffer b( storage, 8);
        b.putInt64(err, 0, 64000);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.remaining() == 8);
        CPPUNIT_ASSERT(b.position() == 0);
        CPPUNIT_ASSERT(b.getInt64(err, 0) == 64000);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.remaining() == 8);
        CPPUNIT_ASSERT(b.position() == 0);
        b.putInt64(err, 64001).flip();
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.getInt64(err) == 64001);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.remaining() == 0);
        CPPUNIT_ASSERT(b.position() == 8);
    }

    void testDouble() {
        char storage[8];
        errType err = errOk;
        ByteBuffer b( storage, 8);
        double value = 64000.124;
        b.putDouble(err, 0, value);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.remaining() == 8);
        CPPUNIT_ASSERT(b.position() == 0);
        double retval = b.getDouble(err, 0);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(::memcmp( &value, &retval, 8) == 0);
        CPPUNIT_ASSERT(b.remaining() == 8);
        CPPUNIT_ASSERT(b.position() == 0);
        value = 64000.126;
        b.putDouble(err, value).flip();
        CPPUNIT_ASSERT(err == errOk);
        retval = b.getDouble(err);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(::memcmp( &value, &retval, 8) == 0);
        CPPUNIT_ASSERT(b.remaining() == 0);
        CPPUNIT_ASSERT(b.position() == 8);
    }

    void testString() {
        char storage[64];
        errType err = errOk;
        std::string value("hello world");
        ByteBuffer b( storage, 64);
        b.putInt32(err, -1).flip();
        CPPUNIT_ASSERT(err == errOk);
        bool wasNull = false;
        std::string retval = b.getString(err, wasNull);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(retval == "");
        CPPUNIT_ASSERT(wasNull);
        b.clear().putString(err, value).flip();
        CPPUNIT_ASSERT(err == errOk);
        retval = b.getString(err, wasNull = false);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(retval == value);
        CPPUNIT_ASSERT(!wasNull);
        b.clear().putString(err, 3, value);
        CPPUNIT_ASSERT(err == errOk);
        retval = b.getString(err, 3, wasNull = false);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(retval == value);
        CPPUNIT_ASSERT(!wasNull);
        b.clear().putInt32(err, -1);
        CPPUNIT_ASSERT(err == errOk);
        retval = b.getString(err, 0, wasNull = false);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(retval == "");
        CPPUNIT_ASSERT(wasNull);
    }

    void testPositionNegative() {
        ByteBuffer b(reinterpret_cast<char*>(1), 32);
        errType err = errOk;
        b.position(err, -1);
        CPPUNIT_ASSERT(err = errIndexOutOfBoundsException);
    }

    void testPositionOver() {
        ByteBuffer b(reinterpret_cast<char*>(1), 32);
        errType err = errOk;
        b.position(err, 33);
        CPPUNIT_ASSERT(err = errIndexOutOfBoundsException);
    }

    void testRemainingAndHasRemaining() {
        errType err = errOk;
        ByteBuffer b(reinterpret_cast<char*>(1), 32);
        CPPUNIT_ASSERT(b.hasRemaining());
        CPPUNIT_ASSERT(b.remaining() == 32);
        b.position(err, 16);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.remaining() == 16);
        CPPUNIT_ASSERT(b.hasRemaining());
        b.limit(err, 24);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.remaining() == 8);
        CPPUNIT_ASSERT(b.hasRemaining());
        b.limit(err, 16);
        CPPUNIT_ASSERT(err == errOk);
        CPPUNIT_ASSERT(b.remaining() == 0);
        CPPUNIT_ASSERT(!b.hasRemaining());
    }

    void testLimitNegative() {
        errType err = errOk;
        ByteBuffer b(reinterpret_cast<char*>(1), 32);
        b.limit(err, -1);
        CPPUNIT_ASSERT(err = errIndexOutOfBoundsException);
    }

    void testLimitOver() {
        errType err = errOk;
        ByteBuffer b(reinterpret_cast<char*>(1), 32);
        b.limit(err, 33);
        CPPUNIT_ASSERT(err = errIndexOutOfBoundsException);
    }

    void testSlice() {
        errType err = errOk;
        ByteBuffer b(reinterpret_cast<char*>(1), 64);
        b.position(err, 16);
        CPPUNIT_ASSERT(err == errOk);
        b.limit(err, 24);
        CPPUNIT_ASSERT(err == errOk);
        ByteBuffer slice = b.slice();
        CPPUNIT_ASSERT(b.position() == 24);
        CPPUNIT_ASSERT(b.remaining() == 0);
        CPPUNIT_ASSERT(slice.position() == 0);
        CPPUNIT_ASSERT(slice.remaining() == 8);
        CPPUNIT_ASSERT(slice.limit() == 8);
        CPPUNIT_ASSERT(reinterpret_cast<uint64_t>(slice.bytes()) == 17);
    }

    void testIsExpandable() {
        ByteBuffer b( reinterpret_cast<char*>(1), 64);
        CPPUNIT_ASSERT(!b.isExpandable());
        CPPUNIT_ASSERT(SharedByteBuffer(new char[32], 32).isExpandable());
        CPPUNIT_ASSERT(ScopedByteBuffer(new char[32], 32).isExpandable());
    }

    void testCopyConstruction() {
        ByteBuffer a( reinterpret_cast<char*>(1), 64);
        ByteBuffer b(a);
        CPPUNIT_ASSERT(a.bytes() == b.bytes());
        CPPUNIT_ASSERT(a.position() == b.position());
        CPPUNIT_ASSERT(a.limit() == b.limit());
        CPPUNIT_ASSERT(a.capacity() == b.capacity());
    }

    void testSharedAndScopedByteBuffer() {
        char *storage = new char[32];
        SharedByteBuffer *shbuffer = new SharedByteBuffer( storage, 32);

        /*
         * Make sure there are no ops
         */
        shbuffer->ensureRemaining(1);
        CPPUNIT_ASSERT(shbuffer->bytes() == storage);
        CPPUNIT_ASSERT(shbuffer->capacity() == 32);
        shbuffer->ensureRemainingExact(1);
        CPPUNIT_ASSERT(shbuffer->bytes() == storage);
        CPPUNIT_ASSERT(shbuffer->capacity() == 32);
        shbuffer->ensureCapacity(1);
        CPPUNIT_ASSERT(shbuffer->bytes() == storage);
        CPPUNIT_ASSERT(shbuffer->capacity() == 32);
        shbuffer->ensureCapacityExact(1);
        CPPUNIT_ASSERT(shbuffer->bytes() == storage);
        CPPUNIT_ASSERT(shbuffer->capacity() == 32);

        errType err = errOk;
        shbuffer->position(err, 32);
        CPPUNIT_ASSERT(err == errOk);
        shbuffer->ensureRemaining(128);
        CPPUNIT_ASSERT(shbuffer->capacity() == 256);
        shbuffer->ensureRemainingExact(533);
        CPPUNIT_ASSERT(shbuffer->remaining() == 533);
        delete shbuffer;

        ScopedByteBuffer *buf = new ScopedByteBuffer( new char[64], 64);
        buf->ensureRemaining(128);
        buf->ensureRemainingExact(533);
        delete buf;
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( ByteBufferTest );
}
