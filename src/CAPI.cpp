#include <algorithm>

#include "CAPI.h"
#include "ClientImpl.h"
#include "TableIterator.h"
#include "Row.hpp"

struct c_client {
   voltdb::ClientConfig* client_config;
   voltdb::Client* client;
};

struct c_procedure {
   voltdb::Procedure* procedure;
   int numParams;
   std::vector<voltdb::Parameter>* parameters;
};

struct c_invocation_response {
   voltdb::InvocationResponse* response;
};

c_client* c_create_client(const char* usrname, const char* pwd,
      const char* hostname, unsigned short port, bool keepConnecting,
      /*voltdb::ClientAuthHashScheme schema,*/ bool enableAbandon, bool enableTimeout,
      int timeoutInSec, bool useSSL) {
   voltdb::ClientConfig* client_config = new voltdb::ClientConfig(
         usrname, pwd, voltdb::HASH_SHA1/*schema*/, enableAbandon,
         enableTimeout, timeoutInSec, useSSL);
   voltdb::Client* client = new voltdb::Client(new voltdb::ClientImpl(*client_config));
   client->createConnection(hostname, port, keepConnecting);
   c_client* rclient = new c_client;
   rclient->client_config = client_config;
   rclient->client = client;
   return rclient;
}

void c_close(c_client* client) {
   if (client != nullptr) {
      client->client->close();
      delete client->client;
      delete client->client_config;
      delete client;
   }
}

c_procedure* c_create_procedure(const char* name,
      int nparams, voltdb::Parameter* params) {
   c_procedure* proc = new c_procedure;
   proc->numParams = nparams;
   proc->parameters = new std::vector<voltdb::Parameter>();
   for (int index = 0; index < nparams; ++index) {
      proc->parameters->push_back(params[index]);
   }
   proc->procedure = new voltdb::Procedure(name, *proc->parameters);
   return proc;
}

c_procedure* c_create_call() {
   c_procedure* proc = new c_procedure;
   proc->numParams = 1;
   proc->parameters = new std::vector<voltdb::Parameter>();
   proc->parameters->push_back(voltdb::WIRE_TYPE_STRING);
   proc->procedure = new voltdb::Procedure("@AdHoc", *proc->parameters);
   return proc;
}

void c_drop_procedure(c_procedure* proc) {
   if (proc != nullptr) {
      delete proc->parameters;
      delete proc->procedure;
      delete proc;
   }
}

c_invocation_response* c_exec_proc(c_client* client, c_procedure* proc, const char** params) {
   if (client == nullptr || proc == nullptr) {
      if(client == nullptr) {
         fprintf(stderr, "Client is null\n");
      }
      if (proc == nullptr) {
         fprintf(stderr, "Procedure is null\n");
      }
      return nullptr;
   } else if (proc->parameters->cend() !=
         std::find_if(proc->parameters->cbegin(), proc->parameters->cend(), [](voltdb::Parameter const& p) {
            return p.m_type != voltdb::WIRE_TYPE_STRING; })) {
      // Support full range of types requires some boiler-plate
      // for serializing/deserializing from char*, hence not
      // supported.
      fprintf(stderr, "Only string typed parameter is supported\n");
      return nullptr;
   } else {
      voltdb::ParameterSet* p = proc->procedure->params();
      for (int index = 0; index < proc->numParams; ++index) {
         p->addString(params[index]);
      }
      c_invocation_response* response = new c_invocation_response;
      response->response =
         new voltdb::InvocationResponse(std::move(client->client->invoke(*proc->procedure)));
      return response;
   }
}

c_invocation_response* c_exec_adhoc(struct c_client* client, struct c_procedure* proc, const char* param) {
   const char* params[] = {param, nullptr};
   return c_exec_proc(client, proc, params);
}

void c_destroy_response(c_invocation_response* response) {
   delete response->response;
   delete response;
}

int c_status_code(c_invocation_response* response) {
   return response->response->statusCode();
}

c_stringified_tables* c_exec_result(c_invocation_response* response) {
   const std::vector<voltdb::Table> tables = response->response->results();
   c_stringified_tables* result = new c_stringified_tables;
   result->num_tables = tables.size();
   if (tables.empty()) {
      result->tables = nullptr;
   } else {
      result->tables = new c_stringified_table[result->num_tables];
      for(int tbl_index = 0; tbl_index < result->num_tables; ++tbl_index) {
         const voltdb::Table& table = tables[tbl_index];
         voltdb::TableIterator iter = table.iterator();
         c_stringified_table* ctable = &result->tables[tbl_index];
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

