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

#include <boost/shared_ptr.hpp>
#include "Distributer.h"
#include "InvocationResponse.hpp"
#include "ProcedureCallback.hpp"

#include "ElasticHashinator.h"

#include "Table.h"
#include "TableIterator.h"
#include "Row.hpp"
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include <boost/scoped_array.hpp>
#include <stdlib.h>
namespace voltdb {

boost::shared_mutex Distributer::m_procInfoLock;
const int Distributer::MP_INIT_PID = 16383;

ProcedureInfo::ProcedureInfo(const std::string & jsonText):PARAMETER_NONE(-1){
    boost::property_tree::ptree pt;
    //todo verify json
    std::stringstream ss(jsonText);
    boost::property_tree::read_json(ss, pt);
    //"{"readOnly":true,"singlePartition":false}
    //{"partitionParameter":0,"readOnly":true,"partitionParameterType":6,"singlePartition":true}

    m_partitionParameter = pt.get("partitionParameter", PARAMETER_NONE);
    m_partitionParameterType = pt.get("partitionParameterType", PARAMETER_NONE);
    //Mandatory
    m_readOnly = pt.get<bool>("readOnly");
    m_multiPart = !pt.get<bool>("singlePartition");
    debug_msg("m_partitionParameter " <<m_partitionParameter << " m_partitionParameterType "<< m_partitionParameterType<< " m_readOnly "<< m_readOnly<< " m_multiPart "<< m_multiPart);
}


int Distributer::parseParameter(ByteBuffer &paramBuffer, int &index){
    int8_t paramType = paramBuffer.getInt8(index++);
    int64_t val = INT64_MIN;
    switch(paramType){
    ///TODO:extend on all data types
        case WIRE_TYPE_TINYINT:
        {
            val = paramBuffer.getInt8(index);
            break;
        }
        case WIRE_TYPE_SMALLINT:
        {
            val = paramBuffer.getInt16(index);
            break;
        }
        case WIRE_TYPE_INTEGER:
        {
            val = paramBuffer.getInt32(index);
            break;
        }
        case WIRE_TYPE_BIGINT:
        case WIRE_TYPE_FLOAT:
        case WIRE_TYPE_TIMESTAMP:
        {
            val = paramBuffer.getInt64(index);
            break;
        }
        case WIRE_TYPE_STRING:
        {
            bool wasNull = false;
            std::string strVal = paramBuffer.getString(index, wasNull);

            if(wasNull)
                return 0;
            return m_hashinator->hashinate((strVal.data()), strVal.size());
        }

        default:
            //not supported
            return -1;
    }

    if (val != INT64_MIN)
        return m_hashinator->hashinate(val);
    return -1;
}


int Distributer::getHashedPartitionForParameter(ByteBuffer &paramBuffer, int parameterId){

    int index = 5;//offset

    bool wasNull;
    std::string name = paramBuffer.getString(index, wasNull);

    //Skip procedure name and size, client data
    index += sizeof(int32_t) + name.size() + sizeof(int64_t);

    //get number of parameters
    paramBuffer.getInt16(index);
    index += 2;

    //partition key must be the first
    if (parameterId > 0)
        return -1;//throw

    return parseParameter(paramBuffer, index);
}

ProcedureInfo* Distributer::getProcedure(const std::string& procName) throw (UnknownProcedureException)
{
    std::map<std::string, ProcedureInfo>::iterator it = m_procedureInfo.find(procName);
    if (it == m_procedureInfo.end())
        return NULL;
    return &it->second;
}

int Distributer::getHostIdByPartitionId(int partitionId)
{
    std::map<int, int>::iterator it = m_PartitionToHostId.find(partitionId);
    if (it == m_PartitionToHostId.end())
        return -1;
    return it->second;
}

void Distributer::handleTopologyNotification(const std::vector<voltdb::Table>& t){
    // If The savedTopoTable is not the same as our notified one, we have to update the hashinator
    if (m_savedTopoTable == t[0]) {
        return;
    }
    updateAffinityTopology(t);
    debug_msg("updateAffinityTopology after notification");
}

void Distributer::updateAffinityTopology(const std::vector<voltdb::Table>& topoTable){
//    Partition, Sites, Leader
//    0,         0:0,   0:0
//    1,         1:0,   1:0
//    2,         2:0,   2:0
//    3,         0:1,   0:1
//    4,         1:1,   1:1
//    5,         2:1,   2:1
//    16383,     2:2,   2:2

    debug_msg("updateAffinityTopology ");
    m_PartitionToHostId.clear();
    voltdb::TableIterator tableIter = topoTable[0].iterator();
    while (tableIter.hasNext())
    {
        voltdb::Row row = tableIter.next();
        debug_msg(row.toString());
        int partitionId = row.getInt32(0);

        std::istringstream ss(row.getString(2));
        int hostId;
        std::string token;
        //parse host Id
        std::getline(ss, token, ':');
        hostId = atoi(token.c_str());

        debug_msg("updateAffinityTopology: partitionId=" <<partitionId << " hostId="<<hostId);
        m_PartitionToHostId.insert(std::pair<int, int >(partitionId, hostId));
    }

    //Get partitions count from second table
    voltdb::TableIterator hashTableIter = topoTable[1].iterator();
    uint32_t tokensCount = 0;
    int32_t realsize = 0;
    voltdb::Row hashRow = hashTableIter.next();

    //get real size of buffer
    hashRow.getVarbinary(1, sizeof(tokensCount), (uint8_t*)&tokensCount, &realsize);

    const std::string hashMode = hashRow.getString(0);
    //Check if token ring is correct
    if (hashMode.compare("ELASTIC") == 0 ) {
        boost::scoped_array<char> tokens(new char[realsize]);
        hashRow.getVarbinary(1, realsize, (uint8_t*)(tokens.get()), &realsize);
        m_hashinator.reset(new ElasticHashinator(tokens.get()));
        m_isElastic = true;
    }else{
        m_isElastic = false;
    }

    //mark update status as finished
    m_isUpdating = false;

    m_savedTopoTable = topoTable[0];
}

void Distributer::updateProcedurePartitioning(const std::vector<voltdb::Table>& procInfoTable){

    debug_msg("updateProcedurePartitioning ");
    boost::unique_lock<boost::shared_mutex> lock(m_procInfoLock);
    m_procedureInfo.clear();

    voltdb::TableIterator tableIter = procInfoTable[0].iterator();

    while (tableIter.hasNext())
    {
        voltdb::Row row = tableIter.next();
        std::string procedureName = row.getString(2);
        std::string jsonString = row.getString(6);

        m_procedureInfo.insert(std::pair<std::string, ProcedureInfo>(procedureName, ProcedureInfo(jsonString)));
    }

}


}





