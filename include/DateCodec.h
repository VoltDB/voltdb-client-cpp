/* This file is part of VoltDB.
 * Copyright (C) 2025 VoltDB Inc.
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
