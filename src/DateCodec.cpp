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

#include "../include/DateCodec.h"

namespace voltdb {

    boost::gregorian::date decodeDate(int32_t encodedDate) {
        int32_t year = (encodedDate >> DATE_YEAR_SHIFT) & TWO_BYTE_MASK;
        int32_t month = (encodedDate >> DATE_MONTH_SHIFT) & ONE_BYTE_MASK;
        int32_t day = (encodedDate >> DATE_DAY_SHIFT) & ONE_BYTE_MASK;
        return boost::gregorian::date(year, month, day);
    }

    int32_t encodeDate(const boost::gregorian::date &date) {
        int32_t year = static_cast<int32_t>(date.year());
        int32_t month = static_cast<int32_t>(date.month());
        int32_t day = static_cast<int32_t>(date.day());
        return (year << DATE_YEAR_SHIFT) | (month << DATE_MONTH_SHIFT) | (day << DATE_DAY_SHIFT);
    }
} // voltdb
