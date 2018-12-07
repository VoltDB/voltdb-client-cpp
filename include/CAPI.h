#pragma once
#include "ClientConfig.h"
#include "Client.h"

#ifdef __cplusplus
extern "C" {
#endif
   struct c_client;
   struct c_procedure;
   struct c_invocation_response;

   struct c_client* c_create_client(const char* usrname, const char* pwd, const char* hostname,
         unsigned short port, bool keepConnecting, /*voltdb::ClientAuthHashScheme schema,*/
         bool enableAbandon, bool enableTimeout, int timeoutInSec, bool useSSL);

   void c_close(struct c_client* config);

   struct c_procedure* c_create_call();
   void c_drop_procedure(c_procedure*);

   struct c_invocation_response* c_exec_proc(struct c_client* client, struct c_procedure* proc, const char** params);
   struct c_invocation_response* c_exec_adhoc(struct c_client* client, struct c_procedure* proc, const char* param);
   int c_status_code(struct c_invocation_response*);
   void c_destroy_response(struct c_invocation_response*);

   struct c_stringified_table {
      int num_cols, num_rows;
      char*** tuples;
   };

   struct c_stringified_tables {
      int num_tables;
      c_stringified_table* tables;
   };

   c_stringified_tables* c_exec_result(struct c_invocation_response*);
   void c_destroy_result(struct c_stringified_tables*);
#ifdef __cplusplus
}
#endif
