/*
 * Hashinator.cpp
 *
 *  Created on: Jan 28, 2014
 *      Author: isolomka
 */

#include <boost/shared_ptr.hpp>
#include "Hashinator.h"
#include "Table.h"
#include "TableIterator.h"
#include "Row.hpp"
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include <stdlib.h>
namespace voltdb {

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

int Hashinator::hashinateLong(int64_t value)
{
    int index = (int) (value ^ (value >> 32));
    return abs(index % m_partitionCount);
}

int Hashinator::hashinateByteArray(const int32_t bufsize, const uint8_t *in_value)
{
    int hashCode = 0;
    int offset = 0;
    for (int ii = 0; ii < bufsize; ii++) {
        hashCode = 31 * hashCode + in_value[offset++];
    }
    return abs(hashCode % m_partitionCount);
}

int Hashinator::parseParameter(ScopedByteBuffer &paramBuffer, int &index){
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
            return hashinateByteArray(strVal.size(), (const uint8_t*)(strVal.c_str()));
        }

        default:
            //not supported
            return -1;
    }

    if (val != INT64_MIN)
        return hashinateLong(val);
    return -1;
}


int Hashinator::getHashedPartitionForParameter(ScopedByteBuffer &paramBuffer, int parameterId, int parameterType){

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

ProcedureInfo* Hashinator::getProcedure(const std::string& procName) throw (UnknownProcedureException)
{
    std::map<std::string, ProcedureInfo>::iterator it = m_procedureInfo.find(procName);
    if (it == m_procedureInfo.end())
        return NULL;
    return &it->second;
}

int Hashinator::getHostIdByPartitionId(int partitionId)
{
    std::map<int, std::vector<int> >::iterator it = m_PartitionToHostId.find(partitionId);
    if (it == m_PartitionToHostId.end())
        return -1;
    return it->second[0];
}


void Hashinator::updateAffinityTopology(const std::vector<voltdb::Table>& topoTable){
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

        std::istringstream ss(row.getString(1));
        std::string item;
        std::vector<int> hosts;
        do
        {
            std::getline(ss, item, ',');
            std::istringstream hostIdSiteId(item);
            std::string token;
            //parse host Id
            std::getline(hostIdSiteId, token, ':');
            hosts.push_back(atoi(token.c_str()));
        }while (!ss.eof());

        m_PartitionToHostId.insert(std::pair<int, std::vector<int> >(partitionId, hosts));
    }

    //Get partitions count from second table
    voltdb::TableIterator hashTableIter = topoTable[1].iterator();
    uint32_t parts;
    int32_t realsize = 0;

    voltdb::Row hashRow = hashTableIter.next();

    hashRow.getVarbinary(1, sizeof(parts), (uint8_t*)&parts, &realsize);
    m_partitionCount = ntohl(parts);

    const std::string& hashType = hashRow.getString(0);

    ///if one of nodes is in elastic scalability --> all cluster is elastic
    if(m_hashType.empty() || (m_hashType.compare("LEGACY") == 0 && hashType.compare("ELASTIC") == 0))
        m_hashType = hashType;

    //mark update status as finished
    m_isUpdating = false;
}

void Hashinator::updateProcedurePartitioning(const std::vector<voltdb::Table>& procInfoTable){

    debug_msg("updateProcedurePartitioning ");
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





