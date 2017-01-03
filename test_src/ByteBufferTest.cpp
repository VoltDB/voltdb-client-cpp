/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
CPPUNIT_TEST_EXCEPTION( checkGetPutIndexNegative, voltdb::OverflowUnderflowException );
CPPUNIT_TEST_EXCEPTION( checkGetPutIndexOver, voltdb::OverflowUnderflowException );
CPPUNIT_TEST_EXCEPTION( checkIndexNegativeIndex, voltdb::IndexOutOfBoundsException );
CPPUNIT_TEST_EXCEPTION( checkIndexOverIndex, voltdb::IndexOutOfBoundsException );
CPPUNIT_TEST_EXCEPTION( checkIndexNegativeLength, voltdb::IndexOutOfBoundsException );
CPPUNIT_TEST_EXCEPTION( checkIndexOverLength, voltdb::IndexOutOfBoundsException );
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
CPPUNIT_TEST_EXCEPTION( testPositionNegative, voltdb::IndexOutOfBoundsException );
CPPUNIT_TEST_EXCEPTION( testPositionOver, voltdb::IndexOutOfBoundsException );
CPPUNIT_TEST( testRemainingAndHasRemaining );
CPPUNIT_TEST_EXCEPTION( testLimitNegative, voltdb::IndexOutOfBoundsException );
CPPUNIT_TEST_EXCEPTION( testLimitOver, voltdb::IndexOutOfBoundsException );
CPPUNIT_TEST( testSlice );
CPPUNIT_TEST( testIsExpandable );
CPPUNIT_TEST_EXCEPTION( testEnsureRemainingThrows, voltdb::NonExpandableBufferException );
CPPUNIT_TEST_EXCEPTION( testEnsureRemainingExactThrows, voltdb::NonExpandableBufferException );
CPPUNIT_TEST_EXCEPTION( testEnsureCapacityThrows, voltdb::NonExpandableBufferException );
CPPUNIT_TEST_EXCEPTION( testEnsureCapacityExactThrows, voltdb::NonExpandableBufferException );
CPPUNIT_TEST( testCopyConstruction );
CPPUNIT_TEST( testSharedAndScopedByteBuffer );
CPPUNIT_TEST_SUITE_END();

public:

    void checkNullBuffer() {
        ByteBuffer b(NULL, 0);
    }

    void checkGetPutIndexValid() {
        ByteBuffer b( reinterpret_cast<char*>(1), 0 );
        b.checkGetPutIndex(0);
    }

    void checkGetPutIndexNegative() {
        ByteBuffer b( reinterpret_cast<char*>(1), 0 );
        b.checkGetPutIndex(-1);
    }

    void checkGetPutIndexOver() {
        ByteBuffer b( reinterpret_cast<char*>(1), 0);
        b.checkGetPutIndex(1);
    }

    void checkIndexNegativeIndex() {
        ByteBuffer b( reinterpret_cast<char*>(1), 0);
        b.checkIndex( -1, 0);
    }

    void checkIndexNegativeLength() {
        ByteBuffer b( reinterpret_cast<char*>(1), 0);
        b.checkIndex( 0, -1);
    }

    void checkIndexOverIndex() {
        ByteBuffer b( reinterpret_cast<char*>(1), 0);
        b.checkIndex( 1, 0);
    }

    void checkIndexOverLength() {
        ByteBuffer b( reinterpret_cast<char*>(1), 0);
        b.checkIndex( 0, 1);
    }

    void checkIndexValid() {
        ByteBuffer b( reinterpret_cast<char*>(1), 0);
        b.checkIndex( 0, 0);
    }

    void testFlip() {
        ByteBuffer b( reinterpret_cast<char*>(1), 32);
        b.position(16);
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
        b.position(16).limit(16);
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
        ByteBuffer b( source, 11);
        b.get( dest, 4);
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
        ByteBuffer b( source, 11);
        b.get( 4, dest, 4);
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
        ByteBuffer b( dest, 64);
        b.put( source, 11);
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
        ByteBuffer b( dest, 64);
        b.put( 5, source, 11);
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
        ByteBuffer b( dest, 64);
        b.put(&empty);
        CPPUNIT_ASSERT(b.position() == 0);
        CPPUNIT_ASSERT(b.remaining() == 64);
        ByteBuffer data( source, 11);
        b.put( &data );
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
        ByteBuffer b( storage, 2);
        b.putInt8( 0, 'a');
        CPPUNIT_ASSERT(b.remaining() == 2);
        CPPUNIT_ASSERT(b.position() == 0);
        CPPUNIT_ASSERT(b.getInt8(0) == 'a');
        CPPUNIT_ASSERT(b.remaining() == 2);
        CPPUNIT_ASSERT(b.position() == 0);
        b.putInt8('b').flip();
        CPPUNIT_ASSERT(b.getInt8() == 'b');
        CPPUNIT_ASSERT(b.remaining() == 0);
        CPPUNIT_ASSERT(b.position() == 1);
    }

    void testInt16() {
        char storage[2];
        ByteBuffer b( storage, 2);
        b.putInt16( 0, 495);
        CPPUNIT_ASSERT(b.remaining() == 2);
        CPPUNIT_ASSERT(b.position() == 0);
        CPPUNIT_ASSERT(b.getInt16(0) == 495);
        CPPUNIT_ASSERT(b.remaining() == 2);
        CPPUNIT_ASSERT(b.position() == 0);
        b.putInt16(500).flip();
        CPPUNIT_ASSERT(b.getInt16() == 500);
        CPPUNIT_ASSERT(b.remaining() == 0);
        CPPUNIT_ASSERT(b.position() == 2);
    }

    void testInt32() {
        char storage[4];
        ByteBuffer b( storage, 4);
        b.putInt32( 0, 64000);
        CPPUNIT_ASSERT(b.remaining() == 4);
        CPPUNIT_ASSERT(b.position() == 0);
        CPPUNIT_ASSERT(b.getInt32(0) == 64000);
        CPPUNIT_ASSERT(b.remaining() == 4);
        CPPUNIT_ASSERT(b.position() == 0);
        b.putInt32(64001).flip();
        CPPUNIT_ASSERT(b.getInt32() == 64001);
        CPPUNIT_ASSERT(b.remaining() == 0);
        CPPUNIT_ASSERT(b.position() == 4);
    }

    void testInt64() {
        char storage[8];
        ByteBuffer b( storage, 8);
        b.putInt64( 0, 64000);
        CPPUNIT_ASSERT(b.remaining() == 8);
        CPPUNIT_ASSERT(b.position() == 0);
        CPPUNIT_ASSERT(b.getInt64(0) == 64000);
        CPPUNIT_ASSERT(b.remaining() == 8);
        CPPUNIT_ASSERT(b.position() == 0);
        b.putInt64(64001).flip();
        CPPUNIT_ASSERT(b.getInt64() == 64001);
        CPPUNIT_ASSERT(b.remaining() == 0);
        CPPUNIT_ASSERT(b.position() == 8);
    }

    void testDouble() {
        char storage[8];
        ByteBuffer b( storage, 8);
        double value = 64000.124;
        b.putDouble( 0, value);
        CPPUNIT_ASSERT(b.remaining() == 8);
        CPPUNIT_ASSERT(b.position() == 0);
        double retval = b.getDouble(0);
        CPPUNIT_ASSERT(::memcmp( &value, &retval, 8) == 0);
        CPPUNIT_ASSERT(b.remaining() == 8);
        CPPUNIT_ASSERT(b.position() == 0);
        value = 64000.126;
        b.putDouble(value).flip();
        retval = b.getDouble();
        CPPUNIT_ASSERT(::memcmp( &value, &retval, 8) == 0);
        CPPUNIT_ASSERT(b.remaining() == 0);
        CPPUNIT_ASSERT(b.position() == 8);
    }

    void testString() {
        char storage[64];
        std::string value("hello world");
        ByteBuffer b( storage, 64);
        b.putInt32(-1).flip();
        bool wasNull = false;
        std::string retval = b.getString(wasNull);
        CPPUNIT_ASSERT(retval == "");
        CPPUNIT_ASSERT(wasNull);
        retval = b.clear().putString(value).flip().getString(wasNull = false);
        CPPUNIT_ASSERT(retval == value);
        CPPUNIT_ASSERT(!wasNull);
        retval = b.clear().putString(3, value).getString(3, wasNull = false);
        CPPUNIT_ASSERT(retval == value);
        CPPUNIT_ASSERT(!wasNull);
        retval = b.clear().putInt32(-1).getString(0, wasNull = false);
        CPPUNIT_ASSERT(retval == "");
        CPPUNIT_ASSERT(wasNull);
    }

    void testPositionNegative() {
        ByteBuffer b(reinterpret_cast<char*>(1), 32);
        b.position(-1);
    }

    void testPositionOver() {
        ByteBuffer b(reinterpret_cast<char*>(1), 32);
        b.position(33);
    }

    void testRemainingAndHasRemaining() {
        ByteBuffer b(reinterpret_cast<char*>(1), 32);
        CPPUNIT_ASSERT(b.hasRemaining());
        CPPUNIT_ASSERT(b.remaining() == 32);
        b.position(16);
        CPPUNIT_ASSERT(b.remaining() == 16);
        CPPUNIT_ASSERT(b.hasRemaining());
        b.limit(24);
        CPPUNIT_ASSERT(b.remaining() == 8);
        CPPUNIT_ASSERT(b.hasRemaining());
        b.limit(16);
        CPPUNIT_ASSERT(b.remaining() == 0);
        CPPUNIT_ASSERT(!b.hasRemaining());
    }

    void testLimitNegative() {
        ByteBuffer b(reinterpret_cast<char*>(1), 32);
        b.limit(-1);
    }

    void testLimitOver() {
        ByteBuffer b(reinterpret_cast<char*>(1), 32);
        b.limit(33);
    }

    void testSlice() {
        ByteBuffer b(reinterpret_cast<char*>(1), 64);
        b.position(16).limit(24);
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

    void testEnsureRemainingThrows() {
        ByteBuffer b( reinterpret_cast<char*>(1), 64);
        b.ensureRemaining(64);
    }

    void testEnsureRemainingExactThrows() {
        ByteBuffer b( reinterpret_cast<char*>(1), 64);
        b.ensureRemainingExact(64);
    }

    void testEnsureCapacityThrows() {
        ByteBuffer b( reinterpret_cast<char*>(1), 64);
        b.ensureCapacity(64);
    }

    void testEnsureCapacityExactThrows() {
        ByteBuffer b( reinterpret_cast<char*>(1), 64);
        b.ensureCapacityExact(64);
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

        shbuffer->position(32);
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
