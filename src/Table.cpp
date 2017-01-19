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
#include <vector>

#include "Table.h"
#include "TableIterator.h"
#include "Row.hpp"
#include "RowBuilder.h"

namespace voltdb {
    const int32_t Table::MAX_TUPLE_LENGTH = 2097152;
    const int8_t Table::DEFAULT_STATUS_CODE = INT8_MIN;

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
        m_rowCountPosition = m_buffer.getInt32(0) + 4;
        m_rowCount = m_buffer.getInt32(m_rowCountPosition);

        m_buffer.position(m_buffer.limit());
    }

    Table::Table(const std::vector<Column> &columns) throw (TableException) {
        if (columns.empty()) {
            throw TableException("Failed to create table. Provided schema can't be empty, "
                    "it must contain at least one column");
        }
        const int initialBuffSize = 8192;
        char *data = new char[initialBuffSize];
        m_buffer = SharedByteBuffer(data, initialBuffSize);

        m_columns.reset(new std::vector<voltdb::Column>(columns));

        // prepare byte buffer
        m_buffer.putInt32(0);       // table header size - start with dummy value
        m_buffer.putInt8(DEFAULT_STATUS_CODE); // status code
        const size_t columnCount = columns.size();
        m_buffer.putInt16((int16_t) columnCount);       // column count

        for (int index = 0; index < columnCount; ++index) {
            assert(columns[index].type() != WIRE_TYPE_INVALID);
            m_buffer.putInt8((int8_t)columns[index].type());    //column type
        }

        for (int index = 0; index < columnCount; ++index) {
            assert((columns[index].name() != std::string()) &&
                    (columns[index].name() != std::string("")));
            m_buffer.putString(columns[index].name());          // column name
        }

        // header size (length-prefixed non-inclusive)
        m_rowCountPosition = m_buffer.position();
        m_buffer.putInt32(0, m_rowCountPosition - 4);   // header size

        m_rowCount = 0;
        m_buffer.putInt32(m_rowCount);                  // O rows to start with
        m_buffer.limit(m_buffer.position());
    }

    int8_t Table::getStatusCode() const{
        return m_buffer.getInt8(4);
    }

    TableIterator Table::iterator() const{
        m_buffer.position(m_rowCountPosition + 4);//skip row count
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
            ostream << m_columns->at(ii).name();
        }
        ostream << std::endl << indent << "Column types: ";
        for (size_t ii = 0; ii < m_columns->size(); ii++) {
            if (ii != 0) {
                ostream << ", ";
            }
            ostream << wireTypeToString(m_columns->at(ii).type());
        }
        ostream << std::endl;
        TableIterator iter = iterator();
        while (iter.hasNext()) {
            Row row = iter.next();
            row.toString(ostream, indent + "    ");
            ostream << std::endl;
        }
    }

    void Table::validateRowScehma(const std::vector<Column>& schema) const throw (InCompatibleSchemaException) {
        if (schema.empty() || schema != *m_columns) {
            throw (InCompatibleSchemaException());
        }
    }

    void Table::addRow(RowBuilder& row) throw (TableException, UninitializedColumnException, InCompatibleSchemaException) {
        const std::vector<Column> schema = row.columns();
        validateRowScehma(schema);
        m_buffer.limit(m_buffer.capacity());

        int32_t serializeRowSize = row.getSerializedSize();
        assert(serializeRowSize <= MAX_TUPLE_LENGTH);
        if (serializeRowSize > MAX_TUPLE_LENGTH) {
            throw TableException("Cannot add row to the table. Row size too large (over 2MB)");
        }

        m_buffer.ensureRemaining(serializeRowSize);
        int32_t serializedSize = row.serializeTo(m_buffer);
        assert(serializedSize == serializeRowSize);

        // update row count
        m_buffer.putInt32(m_rowCountPosition, ++m_rowCount);
        m_buffer.limit(m_buffer.position());
    }

    void Table::operator >> (std::ostream &ostream) const {

        int32_t size = m_buffer.limit();
        ostream.write((const char*)&size, sizeof(size));
        if (size != 0) {
            ostream.write(m_buffer.bytes(), size);
        }
    }

    /*
     * Serialize table to byte buffer. Ensure there is sufficient space
     * in the passed in byte buffer to serialize the data. Needed space
     * to serialize can be obtained by Table::getSerializedSize()
     */
    int32_t Table::serializeTo(ByteBuffer& buffer) throw (TableException){
        buffer.limit(buffer.capacity());
        if (buffer.remaining() < getSerializedSize()) {
            throw TableException("Cannot serialize table as the specified buffer is not large enough. "
                    "Use the getSerializedSize method to determine the necessary size");
        }

        int32_t startPosition = buffer.position();
        buffer.position(startPosition + 4);
        m_buffer.flip();
        buffer.put(&m_buffer);
        int32_t tableSize = buffer.position() - (startPosition + 4);
        buffer.putInt32(startPosition, tableSize);
        buffer.limit(buffer.position());
        return buffer.limit() - startPosition;
    }

    int32_t Table::getSerializedSize() const {
        return 4 + m_buffer.position();
    }

    //Do easy checks first before heavyweight checks.
    bool Table::operator==(const Table& rhs) const {
        if (this == &rhs) return true;
        bool eq = (this->rowCount() == rhs.rowCount() && this->columnCount() == rhs.columnCount());
        if (!eq) return false;
        //Make sure all columns and their order matches.
        if (m_columns != rhs.m_columns) return false;
        //Is underlying buffer same?
        return (m_buffer == rhs.m_buffer);
    }

    //Do easy checks first before heavyweight checks.
    bool Table::operator!=(const Table& rhs) const {
        if (this == &rhs) return false;
        bool noteq = (this->rowCount() != rhs.rowCount() || this->columnCount() != rhs.columnCount());
        if (noteq) return true;
        //Make sure all columns and their order matches.
        if (m_columns != rhs.m_columns) return true;
        //Is underlying buffer same?
        return (m_buffer != rhs.m_buffer);
    }

    static voltdb::SharedByteBuffer readByteBuffer(std::istream &istream) {
        int32_t size;
        istream.read((char*)&size, sizeof(size));
        if (size != 0) {
            boost::shared_array<char> buffer(new char[size]);
            istream.read(buffer.get(), size);
            return voltdb::SharedByteBuffer(buffer, size);
        } else {
            return SharedByteBuffer();
        }
    }

    Table::Table(std::istream &istream) {
        voltdb::SharedByteBuffer buffer = readByteBuffer(istream);
        if (buffer.limit() != 0) {
            *this = Table(buffer);
        }
    }
}
