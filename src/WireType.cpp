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
#include "WireType.h"
#include <cassert>
#include <sstream>

using std::string;

namespace voltdb {
string wireTypeToString(WireType type) {
    switch (type) {
    case WIRE_TYPE_ARRAY:
        return string("ARRAY");
    case WIRE_TYPE_NULL:
        return string("NULL");
    case WIRE_TYPE_TINYINT:
        return string("TINYINT");
    case WIRE_TYPE_SMALLINT:
        return string("SMALLINT");
    case WIRE_TYPE_INTEGER:
        return string("INTEGER");
    case WIRE_TYPE_BIGINT:
        return string("BIGINT");
    case WIRE_TYPE_FLOAT:
        return string("FLOAT");
    case WIRE_TYPE_STRING:
        return string("STRING");
    case WIRE_TYPE_TIMESTAMP:
        return string("TIMESTAMP");
    case WIRE_TYPE_DECIMAL:
        return string("DECIMAL");
    case WIRE_TYPE_VARBINARY:
        return string("VARBINARY");
    case WIRE_TYPE_GEOGRAPHY_POINT:
        return string("GEOGRAPHY_POINT");
    case WIRE_TYPE_GEOGRAPHY:
        return string("GEOGRAPHY");
    default:
        std::ostringstream os;
        os << "<<Unknown Type (" << type << ")>>";
        return os.str();
    }
}
}
