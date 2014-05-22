/*
 * Hashinator.h
 *
 *  Created on: Jan 28, 2014
 *      Author: igosol
 */

#ifndef HASHINATOR_H_
#define HASHINATOR_H_

#include <boost/shared_ptr.hpp>
#include "InvocationResponse.hpp"
#include "ProcedureCallback.hpp"
#include "ByteBuffer.hpp"
#include "Table.h"
#include "Exception.hpp"

#include <map>
#include <string>

//DEBUG ONLY
//#define __DEBUG
#ifdef __DEBUG
#include <iostream>
#include <fstream>

#define debug_msg(text)\
{\
    std::fstream fs;\
    fs.open ("/tmp/hashinator.log", std::fstream::out | std::fstream::app);\
    fs << "[" << __DATE__ << " " << __TIME__ \
    << "] " << __FILE__ << ":" << __LINE__ << " " << text << std::endl;\
    fs.close();\
}

#else

#define debug_msg(text)

#endif

namespace voltdb {

/*
 * Class for saving procedure information obtained by invoke ("@SystemCatalog", "PROCEDURES") procedure
 */
class ProcedureInfo {
public:
    ProcedureInfo(const std::string &jsonText);
    ProcedureInfo():PARAMETER_NONE(-1){}
    ProcedureInfo(const ProcedureInfo &proc):PARAMETER_NONE(proc.PARAMETER_NONE){
        m_multiPart = proc.m_multiPart;
        m_readOnly = proc.m_readOnly;
        m_partitionParameter = proc.m_partitionParameter;
        m_partitionParameterType = proc.m_partitionParameterType;
    }
    virtual ~ProcedureInfo(){}
    bool m_multiPart;
    bool m_readOnly;
    int m_partitionParameter;
    int m_partitionParameterType;

private:
    const int PARAMETER_NONE;
};

class Hashinator {
public:
     void startUpdate(){m_isUpdating = true;}
     bool isUpdating(){return m_isUpdating;}


     void updateAffinityTopology(const std::vector<voltdb::Table>& topoTable);
     void updateProcedurePartitioning(const std::vector<voltdb::Table>& procInfoTable);

     Hashinator():m_partitionCount(0), m_hashType(""), m_isUpdating(false){}

     ~Hashinator(){
         m_procedureInfo.clear();
         m_PartitionToHostId.clear();
     }

     ProcedureInfo* getProcedure(const std::string& procName) throw (UnknownProcedureException);
     int getHashedPartitionForParameter(ScopedByteBuffer &paramBuffer, int parameterId, int parameterType);
     int getHostIdByPartitionId(int partitionId);

     std::string& hashType(){return m_hashType;}

private:
     int hashinateLong(int64_t value);
     int hashinateByteArray(const int32_t bufsize, const uint8_t *in_value);
     int parseParameter(ScopedByteBuffer &paramBuffer, int &index);

     std::map<std::string, ProcedureInfo> m_procedureInfo;
     std::map<int, std::vector<int> > m_PartitionToHostId;
     uint32_t m_partitionCount;
     std::string m_hashType;
     bool m_isUpdating;
};

}

#endif /* HASHINATOR_H_ */


