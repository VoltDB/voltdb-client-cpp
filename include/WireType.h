/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

#ifndef VOLTDB_WIRETYPE_H_
#define VOLTDB_WIRETYPE_H_

#include <string>

namespace voltdb {
enum WireType {
    WIRE_TYPE_INVALID = -98,
    WIRE_TYPE_ARRAY = -99,
    WIRE_TYPE_NULL = 1,
    WIRE_TYPE_TINYINT = 3,
    WIRE_TYPE_SMALLINT = 4,
    WIRE_TYPE_INTEGER = 5,
    WIRE_TYPE_BIGINT = 6,
    WIRE_TYPE_FLOAT = 8,
    WIRE_TYPE_STRING = 9,
    WIRE_TYPE_TIMESTAMP = 11,
    WIRE_TYPE_DECIMAL = 22,
    WIRE_TYPE_VARBINARY = 25
};

std::string wireTypeToString(WireType type);

}
#endif /* VOLTDB_WIRETYPE_H_ */
