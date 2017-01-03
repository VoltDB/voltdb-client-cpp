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
#include <math.h>
#include <cppunit/TestCase.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestFixture.h>
#include "GeographyPoint.hpp"
#include "Geography.hpp"

namespace voltdb {

class GeographyTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE( GeographyTest );
    CPPUNIT_TEST( testToString );
    CPPUNIT_TEST( testEquality );
    CPPUNIT_TEST_SUITE_END();
public:
    void makePolyWithHole(Geography &geo) {
        geo.addEmptyRing()
            << GeographyPoint(0, 0)
            << GeographyPoint(1, 0)
            << GeographyPoint(1, 1)
            << GeographyPoint(0, 1)
            << GeographyPoint(0, 0);
        geo.addEmptyRing()
            << GeographyPoint(0.1, 0.1)
            << GeographyPoint(0.1, 0.9)
            << GeographyPoint(0.9, 0.9)
            << GeographyPoint(0.9, 0.1)
            << GeographyPoint(0.1, 0.1);
    }

    void jitterVertices(Geography &answer, Geography &geo, double epsilon) {
        GeographyPoint offset(epsilon, epsilon);
        for (int ridx = 0; ridx < geo.numRings(); ridx += 1) {
            Geography::Ring &rng = geo.getRing(ridx);
            Geography::Ring &ansRing = answer.addEmptyRing();
            for (int vidx = 0; vidx < rng.numPoints(); vidx += 1) {
                ansRing.addPoint(rng.getPoint(vidx).translate(offset));
            }
        }
    }
    /*
     * It would be possible to test more here.  But this tests pretty much
     * everything that needs to be tested.
     */
    void testToString() {
        std::string poly("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0), (0.1 0.1, 0.1 0.9, 0.9 0.9, 0.9 0.1, 0.1 0.1))");
        Geography geo;
        makePolyWithHole(geo);
        CPPUNIT_ASSERT(poly == geo.toString());
    }

    void testEquality() {
        Geography geo;
        makePolyWithHole(geo);
        Geography nearPoly;
        jitterVertices(nearPoly, geo, 1.0e-15);
        Geography farPoly;
        jitterVertices(farPoly, geo, 1.0e-9);
        /*
         * The near one is 1.0e-15 away.  So it should be
         * less than 1.0e-12 away.
         */
        CPPUNIT_ASSERT(nearPoly.approximatelyEqual(geo, 1.0e-12));
        /*
         * The far one is 1.0e-9 away.  So it should not be
         * within 1.0e-12.
         */
        CPPUNIT_ASSERT(!farPoly.approximatelyEqual(geo, 1.0e-12));
        /*
         * It should be within 1.0e-8, though.
         */
        CPPUNIT_ASSERT(farPoly.approximatelyEqual(geo, 1.0e-8));
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION( GeographyTest );
}
