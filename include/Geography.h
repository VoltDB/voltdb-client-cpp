/*
 * Geography.h
 *
 *  Created on: Feb 29, 2016
 *      Author: bwhite
 */

#ifndef INCLUDE_GEOGRAPHY_H_
#define INCLUDE_GEOGRAPHY_H_
#include <vector>
#include <algorithm>
#include "GeographyPoint.h"

namespace voltdb {

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

        /**
         * Sometimes rings need to be reversed.  We do this by reversing
         * all but the first and last. The first and last should be equal.
         */
        void reverse() {
            PointIterator rev_start = m_points.begin();
            PointIterator rev_end   = m_points.end();
            rev_start++;
            rev_end--;
            std::reverse(rev_start, rev_end);
        }

        bool operator==(const Ring &aOther) const {
            if (numPoints() != aOther.numPoints()) {
                return false;
            }
            for (int idx = 0; idx < numPoints(); idx += 1) {
                if (getPoint(idx) != aOther.getPoint(idx)) {
                    return false;
                }
            }
            return true;
        }

        bool operator!=(const Ring &aOther) const {
            return !operator==(aOther);
        }
    private:
        typedef std::vector<GeographyPoint> PointVector;
        typedef PointVector::iterator       PointIterator;
        PointVector                         m_points;
    };

    Geography() {}

    Geography &addRing(const Ring &aRing) {
        m_rings.push_back(aRing);
        return *this;
    }

    int numRings() const {
        return m_rings.size();
    }

    const Ring &getRing(int idx) const {
        return m_rings[idx];
    }

    bool operator==(const Geography &aOther) const {
        if (numRings() != aOther.numRings()) {
            return false;
        }
        for (int idx = 0; idx < numRings(); idx += 1) {
            if (getRing(idx) != aOther.getRing(idx)) {
                return false;
            }
        }
        return true;
    }

    bool operator !=(const Geography &aOther) const {
        return !operator==(aOther);
    }
private:
    std::vector<Ring> m_rings;

};

inline Geography::Ring &operator<<(Geography::Ring &aRing, const GeographyPoint &aPoint) {
    return aRing.addPoint(aPoint);
}

inline Geography &operator<<(Geography &aGeo, const Geography::Ring &aRing) {
    aGeo.addRing(aRing);
    return aGeo;
}
} /* namespace voltdb */

#endif /* INCLUDE_GEOGRAPHY_H_ */
