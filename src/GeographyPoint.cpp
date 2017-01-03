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

#include <stdint.h>
#include <string>
#include <math.h>
#include <sstream>
#include <iostream>
#include "GeographyPoint.hpp"
#include "ByteBuffer.hpp"

namespace voltdb {
GeographyPoint::GeographyPoint()
    : m_longitude(360.0),
      m_latitude(360.0) {
}

std::string GeographyPoint::toString() const
{
    std::stringstream stream;
    stream << "POINT (" << getLongitude() << " " << getLatitude() << ")";
    return stream.str();
}

GeographyPoint GeographyPoint::fromXYZ(double x, double y, double z)
{
    double degreesPerRadian = (180.0/M_PI);
    double lngRadians = atan2(y, x);
    double latRadians = atan2(z, sqrt(x * x + y * y));
    double longitude = lngRadians * degreesPerRadian;
    double latitude = latRadians * degreesPerRadian;
    return GeographyPoint(longitude, latitude);
}

bool GeographyPoint::approximatelyEqual(const GeographyPoint &aOther,
                                        double epsilon) const {
    double lat = m_latitude;
    double olat = aOther.getLatitude();
    // At the North or South pole, we only care if the
    // other is at the same pole.  We don't care about
    // longitude at all
    if (epsilon == 0) {
        if (fabs(lat) == 90.0) {
            return lat == olat;
        }
    } else {
        /*
         * If |lat| is close to 90, then return
         * true if lat and olat are close to each other,
         * and ignore the longitude.
         */
        if (fabs(fabs(lat) - 90) < epsilon) {
            return fabs(lat - olat) < epsilon;
        }
    }
    double lng = m_longitude;
    double olng = aOther.getLongitude();
    // Normalize the longitudes, so that we
    // don't confuse -180 and 180.
    if ((epsilon == 0 && lng == -180)
            || (epsilon > 0 && (fabs(fabs(lng) - 180.0) < epsilon))) {
        lng = 180.0;
    }
    if ((epsilon == 0 && olng == -180)
            || (epsilon > 0 && fabs(fabs(olng) - 180.0) < epsilon)) {
        olng = 180.0;
    }
    if (epsilon == 0) {
        return lat == olat && lng == olng;
    }
    return fabs(lat - olat) < epsilon
            && fabs(lng - olng) < epsilon;
}

bool GeographyPoint::operator==(const GeographyPoint &aOther) const
{
    return approximatelyEqual(aOther, DEFAULT_EQUALITY_EPSILON);
}

void GeographyPoint::getXYZCoordinates(double &x, double &y, double &z) const
{
    double latRadians = getLatitude() * (M_PI / 180);  // AKA phi
    double lngRadians = getLongitude() * (M_PI / 180); // AKA theta

    double cosPhi = cos(latRadians);
    x = cos(lngRadians) * cosPhi;
    y = sin(lngRadians) * cosPhi;
    z = sin(latRadians);
}

GeographyPoint::GeographyPoint(ByteBuffer &message, int32_t offset, bool &wasNull) {
    (void)deserializeFrom(message, offset, wasNull);
}

int32_t GeographyPoint::deserializeFrom(ByteBuffer &message,
                                        int32_t     offset,
                                        bool       &wasNull)
{
    double longitude = message.getDouble(offset);
    double latitude = message.getDouble(offset + sizeof(double));
    if (longitude == 360.0 && latitude == 360.0) {
        wasNull = true;
    } else {
        wasNull = false;
    }
    m_longitude = longitude;
    m_latitude = latitude;
    return 2 * sizeof(double);
}
}
