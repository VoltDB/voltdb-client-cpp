//
// Created by Tomasz Wiewióra on 09/07/2025.
//

#ifndef VOLTDB_CLIENT_CPP_DATECODEC_H
#define VOLTDB_CLIENT_CPP_DATECODEC_H

#include "boost/date_time.hpp"

namespace voltdb {

    static const int32_t DATE_DAY_SHIFT = 0;
    static const int32_t DATE_MONTH_SHIFT = 8;
    static const int32_t DATE_YEAR_SHIFT = 16;

    static const int32_t ONE_BYTE_MASK = 0xFF;
    static const int32_t TWO_BYTE_MASK = 0xFFFF;


    boost::gregorian::date decodeDate(int32_t encodedDate);

    int32_t encodeDate(const boost::gregorian::date &date);

} // voltdb

#endif //VOLTDB_CLIENT_CPP_DATECODEC_H
