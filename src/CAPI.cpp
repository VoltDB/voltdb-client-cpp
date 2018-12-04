
#include "CAPI.h"
#include "ClientImpl.h"
#include "TableIterator.h"
#include "Row.hpp"

c_client c_create_client(const char* usrname, const char* pwd,
      const char* hostname, unsigned short port, bool keepConnecting,
      /*voltdb::ClientAuthHashScheme schema,*/ bool enableAbandon, bool enableTimeout,
      int timeoutInSec, bool useSSL) {
   voltdb::ClientConfig* client_config = new voltdb::ClientConfig(
         usrname, pwd, voltdb::HASH_SHA1/*schema*/, enableAbandon,
         enableTimeout, timeoutInSec, useSSL);
   voltdb::Client* client = new voltdb::Client(new voltdb::ClientImpl(*client_config));
   client->createConnection(hostname, port, keepConnecting);
   return {
      .client_config = client_config, .client = client
   };
}

void c_close(c_client* client) {
   if (client != nullptr) {
      client->client->close();
      delete client->client;
      delete client->client_config;
   }
}

c_procedure c_create_procedure(const char* name,
      int nparams, voltdb::Parameter* params) {
   c_procedure proc;
   proc.numParams = nparams;
   for (int index = 0; index < nparams; ++index) {
      proc.parameters.push_back(params[index]);
   }
   proc.procedure = new voltdb::Procedure(name, proc.parameters);
   return proc;
}

c_procedure c_create_call() {
   c_procedure proc;
   proc.numParams = 1;
   proc.parameters = std::vector<voltdb::Parameter>(1, voltdb::WIRE_TYPE_STRING);
   proc.procedure = new voltdb::Procedure("@AdHoc", proc.parameters);
   return proc;
}

void c_drop_procedure(c_procedure* proc) {
   if (proc != nullptr) {
      delete proc->procedure;
   }
}

c_invocation_response c_exec_proc(c_client* client, c_procedure* proc, const char** params) {
   if (client == nullptr || proc == nullptr) {
      throw std::runtime_error("Client or Proc unset.");
   } else if (proc->parameters.cend() !=
         std::find_if(proc->parameters.cbegin(), proc->parameters.cend(), [](voltdb::Parameter const& p) {
            return p.m_type != voltdb::WIRE_TYPE_STRING; })) {
      throw std::runtime_error("Only string typed parameter is supported");
   } else {
      voltdb::ParameterSet* p = proc->procedure->params();
      for (int index = 0; index < proc->numParams; ++index) {
         p->addString(params[index]);
      }
      return {.response = client->client->invoke(*proc->procedure)};
   }
}

int c_status_code(c_invocation_response* response) {
   return response->response.statusCode();
}

c_stringified_tables c_exec_result(c_invocation_response* response) {
   const std::vector<voltdb::Table> tables = response->response.results();
   c_stringified_tables result;
   result.num_tables = tables.size();
   if (tables.empty()) {
      result.tables = nullptr;
   } else {
      result.tables = new c_stringified_table[result.num_tables];
      for(int tbl_index = 0; tbl_index < result.num_tables; ++tbl_index) {
         const voltdb::Table& table = tables[tbl_index];
         voltdb::TableIterator iter = table.iterator();
         c_stringified_table* ctable = &result.tables[tbl_index];
         ctable->num_cols = table.columnCount();
         ctable->num_rows = table.rowCount();
         ctable->tuples = new char**[ctable->num_rows];
         for(int row_index = 0; row_index < ctable->num_rows; ++row_index) {
            ctable->tuples[row_index] = new char*[ctable->num_cols];
            voltdb::Row row = iter.next();
            for(int col_index = 0; col_index < ctable->num_cols; ++col_index) {
               const std::string& val = row.get(col_index);
               const size_t len = val.size();
               ctable->tuples[row_index][col_index] = new char[len + 1];
               memcpy(ctable->tuples[row_index][col_index], val.c_str(), len);
               ctable->tuples[row_index][col_index][len] = '\0';
            }
         }
      }
   }
   return result;
}

void c_destroy_result(c_stringified_tables* result) {
   if (result != nullptr) {
      for(int tbl_index = 0; tbl_index < result->num_tables; ++tbl_index) {
         c_stringified_table* tbl = &result->tables[tbl_index];
         for(int row_index = 0; row_index < tbl->num_rows; ++row_index) {
            for(int col_index = 0; col_index < tbl->num_cols; ++col_index) {
               delete[] tbl->tuples[row_index][col_index];
            }
            delete[] tbl->tuples[row_index];
         }
         delete[] tbl->tuples;
         delete tbl;
      }
   }
}

