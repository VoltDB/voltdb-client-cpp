#pragma once
#include "ClientConfig.h"
#include "Client.h"

extern "C" {
   struct c_client{
      voltdb::ClientConfig* client_config;
      voltdb::Client* client;
   };

   c_client c_create_client(const char* usrname, const char* pwd, const char* hostname,
         unsigned short port, bool keepConnecting, /*voltdb::ClientAuthHashScheme schema,*/
         bool enableAbandon, bool enableTimeout, int timeoutInSec, bool useSSL);

   void c_close(c_client* config);

   struct c_procedure {
      voltdb::Procedure* procedure;
      int numParams;
      std::vector<voltdb::Parameter> parameters;
   };

   c_procedure c_create_procedure(const char* name, int nparams, voltdb::Parameter* params);
   c_procedure c_create_call();
   void c_drop_procedure(c_procedure*);

   struct c_invocation_response {
      voltdb::InvocationResponse response;
   };

   c_invocation_response c_exec_proc(c_client* client, c_procedure* proc, const char** params);
   int c_status_code(c_invocation_response*);

   struct c_stringified_table {
      int num_cols, num_rows;
      char*** tuples;
   };

   struct c_stringified_tables{
      int num_tables;
      c_stringified_table* tables;
   };

   c_stringified_tables c_exec_result(c_invocation_response*);
   void c_destroy_result(c_stringified_tables*);
}
