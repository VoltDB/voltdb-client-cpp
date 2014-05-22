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
#include "Table.h"
#include "TableIterator.h"
#include "Row.hpp"

namespace voltdb {
    Table::Table(SharedByteBuffer buffer) : m_buffer(buffer) {
        buffer.position(5);
        size_t columnCount = static_cast<size_t>(buffer.getInt16());
        assert(columnCount > 0);
        boost::shared_ptr<std::vector< voltdb::Column> > columns(
                                new std::vector< voltdb::Column>(columnCount));
        m_columns = columns;

        std::vector<int8_t> types(columnCount);
        for (size_t ii = 0; ii < columnCount; ii++) {
            types[ii] = buffer.getInt8();
        }
        for (size_t ii = 0; ii < columnCount; ii++) {
            bool wasNull = false;
            m_columns->at(ii) = voltdb::Column(buffer.getString(wasNull), static_cast<WireType>(types[ii]));
            assert(!wasNull);
        }
        m_rowStart = m_buffer.getInt32(0) + 4;
        m_rowCount = m_buffer.getInt32(m_rowStart);

        m_buffer.position(m_buffer.limit());
    }

    int8_t Table::getStatusCode() const{
        return m_buffer.getInt8(4);
    }

    TableIterator Table::iterator() const{
        m_buffer.position(m_rowStart + 4);//skip row count
        return TableIterator(m_buffer.slice(), m_columns, m_rowCount);
    }

    int32_t Table::rowCount() const{
        return m_rowCount;
    }

    int32_t Table::columnCount() const{
        return static_cast<int32_t>(m_columns->size());
    }

    std::vector<voltdb::Column> Table::columns() const {
        return *m_columns;
    }


    std::string Table::toString() const {
        std::ostringstream ostream;
        toString(ostream, std::string(""));
        return ostream.str();
    }

    void Table::toString(std::ostringstream &ostream, std::string indent) const {
        ostream << indent << "Table size: " << m_buffer.capacity() << std::endl;
        ostream << indent << "Status code: " << static_cast<int32_t>(getStatusCode()) << std::endl;
        ostream << indent << "Column names: ";
        for (size_t ii = 0; ii < m_columns->size(); ii++) {
            if (ii != 0) {
                ostream << ", ";
            }
            ostream << m_columns->at(ii).m_name;
        }
        ostream << std::endl << indent << "Column types: ";
        for (size_t ii = 0; ii < m_columns->size(); ii++) {
            if (ii != 0) {
                ostream << ", ";
            }
            ostream << wireTypeToString(m_columns->at(ii).m_type);
        }
        ostream << std::endl;
        TableIterator iter = iterator();
        while (iter.hasNext()) {
            Row row = iter.next();
            row.toString(ostream, indent + "    ");
            ostream << std::endl;
        }
    }
}
