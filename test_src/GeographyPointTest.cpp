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

namespace voltdb {
class GeographyPointTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE( GeographyPointTest );
    CPPUNIT_TEST( testCreateAndEquality );
    CPPUNIT_TEST( testPolesAndMeridians  );
    CPPUNIT_TEST( testToString );
    CPPUNIT_TEST_SUITE_END();
    public:

    void testCreateAndEquality() {
        GeographyPoint null_pt;
        CPPUNIT_ASSERT(null_pt.isNull());
        GeographyPoint gp(10, 10);
        CPPUNIT_ASSERT(!gp.isNull());
        CPPUNIT_ASSERT(gp == GeographyPoint(10, 10));
        CPPUNIT_ASSERT(GeographyPoint(10, 10) == gp);
        const double SML_EPS=1.0e-15;
        const double BIG_EPS = 1.0e-9;
        double mult[] = {-1, 0, 1};
        for (int iidx = 0; iidx < 3; iidx += 1) {
            for (int jidx = 0; jidx < 3; jidx += 1) {
                double lngmult = mult[iidx];
                double latmult = mult[jidx];
                if (jidx != 1 || iidx != 1) {
                    GeographyPoint np(10 + lngmult*SML_EPS, 10 + latmult*SML_EPS);
                    GeographyPoint fp(10 + lngmult*BIG_EPS, 10 + latmult*BIG_EPS);
                    CPPUNIT_ASSERT(np == gp);
                    if (fp == gp) {
                        std::cout << "iidx = " << iidx
                                << ", jidx = " << jidx
                                << ", lngmult = " << lngmult
                                << ", latmult = " << latmult
                                << "\n";
                        std::cout << "fp = " << fp.toString()
                                  << ", gp = " << gp.toString()
                                  << ", diff = ("
                                  << fabs(fp.getLongitude() - gp.getLongitude())
                                  << ", "
                                  << fabs(fp.getLatitude() - gp.getLatitude())
                                  << ")\n";
                    }
                    CPPUNIT_ASSERT(fp != gp);
                    CPPUNIT_ASSERT(gp.approximatelyEqual(np, BIG_EPS));
                    CPPUNIT_ASSERT(!gp.approximatelyEqual(np, SML_EPS));
                }
            }
        }
    }

    void testPolesAndMeridians() {
        GeographyPoint northpoleW  (- 90,  90);
        GeographyPoint northpoleE  (  90,  90);
        GeographyPoint northpoleM  (   0,  90); // meridian
        GeographyPoint northpoleAMP( 180,  90); // positive anti meridian
        GeographyPoint northpoleAMN(-180,  90); // negative anti meridian
        GeographyPoint southpoleW  (- 90, -90);
        GeographyPoint southpoleE  (  90, -90);
        GeographyPoint southpoleM  (   0, -90);
        GeographyPoint southpoleAMP( 180, -90);
        GeographyPoint southpoleAMN(-180, -90);
        CPPUNIT_ASSERT(northpoleW == northpoleE);
        CPPUNIT_ASSERT(northpoleE == northpoleM);
        CPPUNIT_ASSERT(northpoleM == northpoleAMN);
        CPPUNIT_ASSERT(northpoleAMN == northpoleAMP);

        CPPUNIT_ASSERT(southpoleW != northpoleE);
        CPPUNIT_ASSERT(southpoleE != northpoleM);
        CPPUNIT_ASSERT(southpoleM != northpoleAMN);
        CPPUNIT_ASSERT(southpoleAMN != northpoleAMP);

        CPPUNIT_ASSERT(southpoleW == southpoleE);
        CPPUNIT_ASSERT(southpoleE == southpoleM);
        CPPUNIT_ASSERT(southpoleM == southpoleAMN);
        CPPUNIT_ASSERT(southpoleAMN == southpoleAMP);

        CPPUNIT_ASSERT(southpoleW != northpoleE);
        CPPUNIT_ASSERT(southpoleE != northpoleM);
        CPPUNIT_ASSERT(southpoleM != northpoleAMN);
        CPPUNIT_ASSERT(southpoleAMN != northpoleAMP);

        GeographyPoint pmeridian(180.0, 45.0);
        GeographyPoint nmeridian(-180.0, 45.0);
        CPPUNIT_ASSERT(pmeridian == nmeridian);
    }

    void testToString() {
        std::string ptStr("POINT (10 10)");
        GeographyPoint pt(10, 10);
        CPPUNIT_ASSERT(ptStr == pt.toString());
    }

};
CPPUNIT_TEST_SUITE_REGISTRATION( GeographyPointTest );
}
