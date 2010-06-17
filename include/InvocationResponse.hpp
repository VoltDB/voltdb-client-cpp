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

#ifndef VOLTDB_INVOCATIONRESPONSE_HPP_
#define VOLTDB_INVOCATIONRESPONSE_HPP_
#include <boost/shared_array.hpp>
#include <vector>
#include <sstream>
#include <boost/shared_ptr.hpp>
#include "ByteBuffer.hpp"
#include "Table.h"

namespace voltdb {
class InvocationResponse {
public:
    InvocationResponse(boost::shared_array<char> data, int32_t length) {
        SharedByteBuffer buffer(data, length);
        int8_t version = buffer.getInt8();
        assert(version == 0);
        m_clientData = buffer.getInt64();
        int8_t presentFields = buffer.getInt8();
        m_statusCode =  buffer.getInt8();
        bool wasNull = false;
        if ((presentFields & (1 << 5)) != 0) {
            m_statusString = buffer.getString(wasNull);
        }
        m_appStatusCode = buffer.getInt8();
        if ((presentFields & (1 << 7)) != 0) {
            m_appStatusString = buffer.getString(wasNull);
        }
        assert(!wasNull);
        m_clusterRoundTripTime = buffer.getInt32();
        if ((presentFields & (1 << 6)) != 0) {
            int32_t position = buffer.position();
            buffer.position(position + buffer.getInt32());
        }
        size_t resultCount = static_cast<size_t>(buffer.getInt16());
        m_results = std::vector<boost::shared_ptr<voltdb::Table> >(resultCount);
        int32_t startLimit = buffer.limit();
        for (size_t ii = 0; ii < resultCount; ii++) {
            int32_t tableLength = buffer.getInt32();
            assert(tableLength >= 4);
            buffer.limit(buffer.position() + tableLength);
            m_results[ii] = boost::shared_ptr<voltdb::Table>(new voltdb::Table(buffer.slice()));
            buffer.limit(startLimit);
        }
    }

    int64_t clientData() { return m_clientData; }
    int8_t statusCode() { return m_statusCode; }
    std::string statusString() { return m_statusString; }
    int8_t appStatusCode() { return m_appStatusCode; }
    std::string appStatusString() { return m_appStatusString; }
    int32_t clusterRoundTripTime() { return m_clusterRoundTripTime; }
    std::vector<boost::shared_ptr<voltdb::Table> > results() { return m_results; }

    std::string toString() {
        std::ostringstream ostream;
        ostream << "Status: " << static_cast<int32_t>(statusCode()) << ", " << statusString() <<  std::endl;
        ostream << "App Status: " << static_cast<int32_t>(appStatusCode()) << ", " << appStatusString() << std::endl;
        ostream << "Client Data: " << clientData() << std::endl;
        ostream << "Cluster Round Trip Time: " << clusterRoundTripTime() << std::endl;
        for (size_t ii = 0; ii < m_results.size(); ii++) {
            ostream << "Result Table " << ii << std::endl;
            m_results[ii]->toString(ostream, std::string("    "));
        }
        return ostream.str();
    }

private:
    int64_t m_clientData;
    int8_t m_statusCode;
    std::string m_statusString;
    int8_t m_appStatusCode;
    std::string m_appStatusString;
    int32_t m_clusterRoundTripTime;
    std::vector<boost::shared_ptr<voltdb::Table> > m_results;
};
}

#endif /* VOLTDB_INVOCATIONRESPONSE_HPP_ */
