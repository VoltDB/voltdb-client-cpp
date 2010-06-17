/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

#ifndef VOLTTABLE_H_
#define VOLTTABLE_H_

#include "ByteBuffer.hpp"
#include <boost/shared_ptr.hpp>
#include <vector>
#include "Column.hpp"

namespace voltdb {
class TableIterator;
class RowBuilder;
class Table {
    friend class RowBuilder;
public:
    Table(SharedByteBuffer buffer);
    //Table(std::vector<Column> columns);
    TableIterator iterator();
    //void addRow(RowBuilder *builder);
    //int32_t getSerializedSize();
    //void serializeTo(ByteBuffer *buffer);
    int8_t getStatusCode();
    //int8_t setStatusCode(int8_t code);
private:
    boost::shared_ptr<std::vector<voltdb::Column> > m_columns;
    int32_t m_rowStart;
    int32_t m_rowCount;
    voltdb::SharedByteBuffer m_buffer;
};
}

#endif /* VOLTTABLE_H_ */
