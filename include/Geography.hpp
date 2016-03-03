/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#ifndef INCLUDE_GEOGRAPHY_H_
#define INCLUDE_GEOGRAPHY_H_
#include <vector>
#include "GeographyPoint.hpp"


namespace voltdb {

class ByteBuffer;

struct Geography {
    struct Ring {
        Ring() {}

        void clear() {
            m_points.clear();
        }

        Ring &addPoint(const GeographyPoint &aPoint) {
            m_points.push_back(aPoint);
            return *this;
        }

        int numPoints() const {
            return m_points.size();
        }

        const GeographyPoint &getPoint(int idx) const {
            return m_points[idx];
        }

        GeographyPoint &getPoint(int idx) {
            return m_points[idx];
        }

        /**
         * Sometimes rings need to be reversed.  We do this by reversing
         * all but the first and last. The first and last should be equal.
         */
        void reverse();

        bool operator==(const Ring &aOther) const {
            return approximatelyEqual(aOther, 0.0);
        }

        bool operator!=(const Ring &aOther) const {
            return !operator==(aOther);
        }

        bool approximatelyEqual(const Ring &rhs, double epsilon) const;

        std::string toString() const;
                       
        void serializeTo(ByteBuffer &buffer, bool reverseit) const;
    private:
        typedef std::vector<GeographyPoint> PointVector;
        typedef PointVector::iterator       PointIterator;
        typedef PointVector::const_iterator PointConstIterator;
        PointVector                         m_points;
    };

    Geography() {}

    /**
     * Add an already existing ring.  This is used in
     * operator<< to add rings to Geographies.  A much
     * more efficient location is to sue addEmptyRing()
     * (c.f. below).
     */
    Geography &addRing(const Ring &aRing) {
        m_rings.push_back(aRing);
        return *this;
    }
    /**
     * Add a new ring, then return a reference to the ring we added.
     * This is used typically like this:
     *   polygon.addEmptyRing() << GeographyPoint(0, 0)
     *                          << GeographyPoint(1, 0)
     *                          ...;
     */
    Ring &addEmptyRing() {
        // It's unfortunate that we can't use C++11
        // here.  This is perfect for emplacement new.
        m_rings.push_back(Ring());
        return m_rings[m_rings.size()-1];
    }

    int numRings() const {
        return m_rings.size();
    }

    const Ring &getRing(int idx) const {
        return m_rings[idx];
    }

    bool operator==(const Geography &aOther) const {
        return approximatelyEqual(aOther, 0.0);
    }

    bool operator !=(const Geography &aOther) const {
        return !operator==(aOther);
    }

    /**
     * Return true iff the the given geography is equal to this
     * one, but allow the coordinates to differ by something less
     * than epsilon.  If epsilon is 0.0, we require strict
     * equality.
     */
    bool approximatelyEqual(const Geography &rhs, double epsilon) const ;

    /**
     * Return the size of this geography, including the 4 byte
     * size field.
     */
    int32_t getSerializedSize() const;

    /**
     * Serialize this geography to the given buffer.  This function
     * writes the size.  This returns the amount of data actually
     * written, as a check on getSerializedSize.
     */
    int32_t serializeTo(ByteBuffer &buffer) const;
    /**
     * Deserialize a geography value from a ByteBuffer.  We
     * read the size as well.  Note that we never use the
     * position() member function in this call, even implicitly.
     * we always calculate where our offsets are.  Consequently
     * the position is not changed by the function.
     *
     * Return the number of bytes actually used.
     */
    int32_t deserializeFrom(ByteBuffer &aData,
                            int32_t firstOffset,
                            bool &aWasNull);
    /**
     * Generate some WKT text for this buffer.   This is mostly
     * useful for debugging.
     */
    std::string toString();
    /**
     * The null polygon has no rings.
     */
    bool isNull() const {
        return m_rings.size() == 0;
    }
    /**
     * Make this Geography be null.
     */
    void makeNull() {
        m_rings.clear();
    }
private:
    typedef std::vector<Ring>          RingVector;
    typedef RingVector::iterator       RingIterator;
    typedef RingVector::const_iterator RingConstIterator;
    std::vector<Ring> m_rings;
};

/**
 * This is useful to add a point to a ring.
 */
inline Geography::Ring &operator<<(Geography::Ring &aRing, const GeographyPoint &aPoint) {
    return aRing.addPoint(aPoint);
}

/**
 * This is useful to add a ring to a Geography.
 */
inline Geography &operator<<(Geography &aGeo, const Geography::Ring &aRing) {
    aGeo.addRing(aRing);
    return aGeo;
}
} /* namespace voltdb */

#endif /* INCLUDE_GEOGRAPHY_H_ */
