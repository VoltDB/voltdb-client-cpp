/*
 * GeographyPoint.h
 *
 *  Created on: Feb 29, 2016
 *      Author: bwhite
 */

#ifndef INCLUDE_GEOGRAPHYPOINT_H_
#define INCLUDE_GEOGRAPHYPOINT_H_
#include <math.h>

namespace voltdb {

/**
 * Represent a geography point. There are no virtual functions here,
 * so there is no destructor at all.  The default one is good enough
 * for us.
 */
struct GeographyPoint {
    GeographyPoint(double aLongitude, double aLatitude)
        : m_longitude(aLongitude),
          m_latitude(aLatitude) {}

    double getLongitude() const {
        return m_longitude;
    }

    double getLatitude() const {
        return m_latitude;
    }

    bool operator==(const GeographyPoint &aOther) const {
        return getLongitude() == aOther.getLongitude() && getLatitude() == aOther.getLatitude();
    }

    bool operator!=(const GeographyPoint &aOther) const {
        return !operator==(aOther);
    }
    static GeographyPoint fromXYZ(double x, double y, double z) {
        double degreesPerRadian = (180.0/M_PI);
        double lngRadians = atan2(y, x);
        double latRadians = atan2(z, sqrt(x * x + y * y + z * z));
        double longitude = lngRadians * degreesPerRadian;
        double latitude = latRadians * degreesPerRadian;
        return GeographyPoint(longitude, latitude);
    }
private:
    double m_longitude;
    double m_latitude;
};

} /* namespace voltdb */

#endif /* INCLUDE_GEOGRAPHYPOINT_H_ */
