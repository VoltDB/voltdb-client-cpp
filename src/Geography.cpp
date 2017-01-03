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

#include <iostream>
#include <sstream>
#include <stdint.h>
#include "Geography.hpp"
#include "ByteBuffer.hpp"

namespace voltdb {
void Geography::Ring::reverse()
{
    PointIterator rev_start = m_points.begin();
    PointIterator rev_end   = m_points.end();
    rev_start++;
    rev_end--;
    std::reverse(rev_start, rev_end);
}

bool Geography::Ring::approximatelyEqual(const Ring &rhs, double epsilon) const {
    if (numPoints() != rhs.numPoints()) {
        return false;
    }
    for (int idx = 0; idx < numPoints(); idx += 1) {
        if (!getPoint(idx).approximatelyEqual(rhs.getPoint(idx), epsilon)) {
            return false;
        }
    }
    return true;
}

std::string Geography::Ring::toString() const {
    std::stringstream stream;
    stream << "(";
    std::string sep("");
    for (int idx = 0; idx < numPoints(); idx += 1) {
        const GeographyPoint &pt = getPoint(idx);
        stream << sep
               << pt.getLongitude()
               << " "
               << pt.getLatitude();
        sep = ", ";
    }
    stream << ")";
    return stream.str();
}

void Geography::Ring::serializeTo(ByteBuffer &buffer, bool reverseit) const {
    buffer.putInt8(0);
    buffer.putInt32(m_points.size() - 1);
    int start = (reverseit ? m_points.size() - 1 : 0);
    int end   = (reverseit ? 0 : m_points.size() - 1);
    int delta = (reverseit ? -1 : 1);
    for (int idx = start; idx != end; idx += delta) {
        double x, y, z;
        m_points[idx].getXYZCoordinates(x, y, z);
        buffer.putDouble(x);
        buffer.putDouble(y);
        buffer.putDouble(z);
    }
    char zeros[38];
    memset(zeros, 0, sizeof(zeros));
    buffer.put(zeros, sizeof(zeros));
}

Geography::Geography(ByteBuffer &message, int32_t offset, bool &wasNull)
    : m_rings()
{
    // deserializeFrom returns a value, which we have to
    // explicitly throw away here, or the compiler will
    // gripe.
    (void)deserializeFrom(message, offset, wasNull);
}

bool Geography::approximatelyEqual(const Geography &rhs, double epsilon) const {
    if (numRings() != rhs.numRings()) {
        return false;
    }
    for (uint32_t idx = 0; idx < m_rings.size(); idx += 1) {
        if (!getRing(idx).approximatelyEqual(rhs.getRing(idx), epsilon)) {
            return false;
        }
    }
    return true;
}

int32_t Geography::getSerializedSize() const {
    // Null Geography values are
    // just 4 bytes long - just enough
    // for the -1.
    if (isNull()) {
        return 4;
    }
    int32_t answer
        = 4 // size
        + 3 // 3 internal bytes
        + 4 // Count of the number of rings
        + 33; // Trailing overhead
    for (RingConstIterator idx = m_rings.begin();
         idx != m_rings.end();
         idx++) {
        // Each ring has 3 coordinates for each point
        // plus 5 + 38 bytes of overhead.  Note that we
        // will not serialize the duplicated first point.
        answer += (idx->numPoints()-1)*3*sizeof(double) + 5 + 38;
    }
    return answer;
}

int32_t Geography::serializeTo(ByteBuffer &buffer) const {
    // Remember where we start, so that we can
    // write the size.  We keep track of how much
    // we actually write and return it, so that the
    // caller can check that this and getSerializedSize
    // agree.
    int32_t start = buffer.position();
    if (isNull()) {
        buffer.putInt32(-1);
        return 4;
    }

    // Save space for the size.  We don't
    // really know what it is now, so we
    // well seek back to it after we are done.
    buffer.position(buffer.position() + 4);
    // We write three bytes of overhead, followed by the
    // number of rings.
    buffer.putInt8(0);
    buffer.putInt8(1);
    buffer.putInt8(0);
    buffer.putInt32(m_rings.size());
    bool reverseit = false;
    for (RingConstIterator idx = m_rings.begin();
         idx != m_rings.end();
         idx++) {
        // Serialize each ring.
        idx->serializeTo(buffer, reverseit);
        reverseit = true;
    }

    // We write 33 bytes of overhead.
    char zeros[33];
    memset(zeros, 0, sizeof(zeros));
    buffer.put(zeros, sizeof(zeros));

    // Write the size where we started.
    int32_t size = buffer.position() - start;
    buffer.putInt32(start, size-4);
    return size;
}

int32_t Geography::deserializeFrom(ByteBuffer &aData,
                                   int32_t first_offset,
                                   bool &aWasNull)
{
    int32_t offset = first_offset;
    // The first four bytes are the length.  We read them
    // only so we can validate the read.  The length does
    // not include them.
    int32_t len = aData.getInt32(offset);
    if (len == -1) {
        aWasNull = true;
        makeNull();
        return 4;
    } else {
        aWasNull = false;
    }
    int32_t last_offset = first_offset + 4 + len;
    offset += sizeof(int32_t);
    assert(offset < last_offset);
    // The first seven bytes are somewhat overhead, but they contain
    // the number of rings.
    int32_t numRings = aData.getInt32(offset + 3);
    offset += 7;
    assert(offset < last_offset);
    assert(0 <= numRings);
    for (int idx = 0; idx < numRings; idx += 1) {
        Ring &rng = addEmptyRing();
        rng.clear();
        // The first byte is protocol overhead.
        // See the VoltDB Wire Protocol Specification.
        offset += 1;
        assert(offset < last_offset);
        // The next 4 bytes are the number of vertices.
        int numVerts = aData.getInt32(offset);
        offset += 4;
        assert(offset < last_offset);
        assert(3 <= numVerts);
        for (; numVerts > 0; numVerts -= 1) {
            double x = aData.getDouble(offset);
            double y = aData.getDouble(offset + sizeof(double));
            double z = aData.getDouble(offset + 2 * sizeof(double));
            GeographyPoint pt = GeographyPoint::fromXYZ(x, y, z);
            rng.addPoint(pt);
            offset += 3*sizeof(double);
            assert(offset < last_offset);
        }
        // There are 38 bytes of overhead at the end.
        // See the VoltDB Wire Protocol Specification.
        offset += 38;
        assert(offset < last_offset);
        // Close the loop.
        rng.addPoint(rng.getPoint(0));
        if (0 < idx) {
            rng.reverse();
        }
        // We don't have to add rng because we added it
        // when it was created.
    }
    // There are 33 bytes of overhead at the end.
    // See the VoltDB Wire Protocol Specification.
    assert(offset+33 == last_offset);
    return len;
}

std::string Geography::toString()
{
    std::stringstream stream;
    stream << "POLYGON (";
    std::string sep("");
    for (int idx = 0; idx < numRings(); idx += 1) {
        stream << sep << getRing(idx).toString();
        sep = ", ";
    }
    stream << ")";
    return stream.str();
}

}

