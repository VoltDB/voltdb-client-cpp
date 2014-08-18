/*
 * Distributer.h
 *
 *  Created on: Jan 28, 2014
 *      Author: igosol
 */

#ifndef DISTRIBUTER_H_
#define DISTRIBUTER_H_

#include <boost/scoped_ptr.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include "TheHashinator.h"
#include "Table.h"
#include "ByteBuffer.hpp"
#include "Exception.hpp"
#include <map>
#include <string>



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

class Distributer{
public:
     void startUpdate(){m_isUpdating = true;}
     bool isUpdating(){return m_isUpdating;}
     bool isElastic(){return m_isElastic;}

     void updateAffinityTopology(const std::vector<voltdb::Table>& topoTable);
     void updateProcedurePartitioning(const std::vector<voltdb::Table>& procInfoTable);

     Distributer(): m_isUpdating(false), m_isElastic(true){}

     virtual ~Distributer(){
         m_procedureInfo.clear();
         m_PartitionToHostId.clear();
     }

     ProcedureInfo* getProcedure(const std::string& procName) throw (UnknownProcedureException);
     int getHashedPartitionForParameter(ByteBuffer &paramBuffer, int parameterId);
     int getHostIdByPartitionId(int partitionId);
     static const int MP_INIT_PID;

private:
     int parseParameter(ByteBuffer &paramBuffer, int &index);

     std::map<std::string, ProcedureInfo> m_procedureInfo;
     std::map<int, int> m_PartitionToHostId;
     bool m_isUpdating;
     bool m_isElastic;
     boost::scoped_ptr<TheHashinator> m_hashinator;

     static boost::shared_mutex m_procInfoLock;
};

}

#endif /* HASHINATOR_H_ */


