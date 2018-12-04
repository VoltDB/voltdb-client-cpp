/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

#define __STDC_CONSTANT_MACROS
#define __STDC_LIMIT_MACROS

#include <vector>
#include "Client.h"
#include "Table.h"
#include "TableIterator.h"
#include "Row.hpp"
#include "WireType.h"
#include "Parameter.hpp"
#include "ParameterSet.hpp"
#include "ClientConfig.h"

#include "CAPI.h"

void print_result(c_stringified_tables* r) {
   for(int tbl_index = 0; tbl_index < r->num_tables; ++tbl_index) {
      std::cout<<tbl_index<<" th table:"<<std::endl;
      const c_stringified_table& tbl = r->tables[tbl_index];
      for(int row_index = 0; row_index < tbl.num_rows; ++row_index) {
         for(int col_index = 0; col_index < tbl.num_cols; ++col_index) {
            std::cout<<tbl.tuples[row_index][col_index]<<"\t";
         }
         std::cout<<std::endl;
      }
      std::cout<<std::endl;
   }
}

int main(int argc, char **argv) {
    /*
     * Instantiate a client and connect to the database.
     * SHA-256 can be used as of VoltDB5.2 by specifying voltdb::HASH_SHA256
     */
    voltdb::ClientConfig config("myusername", "mypassword", voltdb::HASH_SHA1);
    voltdb::Client client = voltdb::Client::create(config);
    client.createConnection("localhost");

    /*
     * Describe the stored procedure to be invoked
     */
    std::vector<voltdb::Parameter> parameterTypes(3);
    parameterTypes[0] = voltdb::Parameter(voltdb::WIRE_TYPE_STRING);
    parameterTypes[1] = voltdb::Parameter(voltdb::WIRE_TYPE_STRING);
    parameterTypes[2] = voltdb::Parameter(voltdb::WIRE_TYPE_STRING);
    {

       c_client client = c_create_client("myusername", "mypassword", "localhost", 21212, false,
             /*voltdb::HASH_SHA1,*/ false, false, 10, false);
       c_procedure adhoc = c_create_call();
       std::vector<std::string> queries{
          "DROP TABLE foo IF EXISTS;",
          "CREATE TABLE foo(i int);",
          "INSERT INTO foo VALUES(12);",
          "INSERT INTO foo VALUES(14);",
          "INSERT INTO foo VALUES(18);",
          "SELECT * FROM foo LIMIT 5;"
       };
       for (std::string const& s : queries) {
          const char* params[] = {s.c_str(), nullptr};
          voltdb::InvocationResponse response = c_exec_proc(&client, &adhoc, params).response;
          if (response.failure()) { std::cout <<response.toString() << std::endl; return -1; }
       }
       const char* params[] = {queries[5].c_str(), nullptr};
       c_invocation_response response = c_exec_proc(&client, &adhoc, params);
       c_stringified_tables result = c_exec_result(&response);
       print_result(&result);
       c_destroy_result(&result);
       c_drop_procedure(&adhoc);
       c_close(&client);

       /*
          voltdb::Procedure adhoc("@AdHoc", std::vector<voltdb::Parameter>(1, voltdb::WIRE_TYPE_STRING));
          voltdb::ParameterSet* params = adhoc.params();

          params->addString("CREATE TABLE foo (i int);");
          voltdb::InvocationResponse response = client.invoke(adhoc);
          if (response.failure()) { std::cout <<response.toString() << std::endl; return -1; }

          params->addString("INSERT INTO foo VALUES(12);");
          response = client.invoke(adhoc);
          if (response.failure()) { std::cout <<response.toString() << std::endl; return -1; }

          params->addString("SELECT * from foo LIMIT 5;");
          response = client.invoke(adhoc);
          if (response.failure()) { std::cout <<response.toString() << std::endl; return -1; }
          std::cout << response.toString();
          return 0;
          */

    }
    voltdb::Procedure procedure("Insert", parameterTypes);

    voltdb::InvocationResponse response;
    /*
     * Load the database.
     */
    voltdb::ParameterSet* params = procedure.params();
    params->addString("English").addString("Hello").addString("World");
    response = client.invoke(procedure);
    if (response.failure()) { std::cout << response.toString() << std::endl; return -1; }

    params->addString("French").addString("Bonjour").addString("Monde");
    response = client.invoke(procedure);
    if (response.failure()) { std::cout << response.toString(); return -1; }

    params->addString("Spanish").addString("Hola").addString("Mundo");
    response = client.invoke(procedure);
    if (response.failure()) { std::cout << response.toString(); return -1; }

    params->addString("Danish").addString("Hej").addString("Verden");
    response = client.invoke(procedure);
    if (response.failure()) { std::cout << response.toString(); return -1; }

    params->addString("Italian").addString("Ciao").addString("Mondo");
    response = client.invoke(procedure);
    if (response.failure()) { std::cout << response.toString(); return -1; }

    /*
     * Describe procedure to retrieve message
     */
    parameterTypes.resize( 1, voltdb::Parameter(voltdb::WIRE_TYPE_STRING));
    voltdb::Procedure selectProc("Select", parameterTypes);

    /*
     * Retrieve the message
     */
    selectProc.params()->addString("Spanish");
    response = client.invoke(selectProc);

    /*
     * Print the response
     */
    std::cout << response.toString();

    if (response.statusCode() != voltdb::STATUS_CODE_SUCCESS) {
        return -1;
    } else {
        return 0;
    }
}

