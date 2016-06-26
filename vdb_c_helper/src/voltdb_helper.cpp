/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
#include <iostream>
#include "Client.h"

#include "StatusListener.h"
#include "Parameter.hpp"
#include "ParameterSet.hpp"
#include "Procedure.hpp"
#include "WireType.h"
#include "ProcedureCallback.hpp"
#include "InvocationResponse.hpp"
#include "ClientConfig.h"

#include "header/voltdb_helper.h"



static void fire ( void * client,  char * query, voltdb::InvocationResponse* resp);

void * vdb_create_client_config( char* uname, char* passwd, unsigned int conn_type )
{
	voltdb::ClientConfig* cc = NULL;

	try
	{
		cc =  new voltdb::ClientConfig(uname, passwd, voltdb::HASH_SHA1);
	}
	catch (...)
	{
		return NULL;
	}

	return cc;
}


void * vdb_create_client( void * cc, char* host )
{
	voltdb::Client* c = NULL;
	try
	{
		 c = voltdb::Client::create(true, *(voltdb::ClientConfig*)cc);
		(*c).createConnection(host);
	}
	catch(...)
	{
		return NULL;
	}

	return c;

}

int vdb_fire_upsert_query( void * client,  char * query, char** resp)
{
	voltdb::InvocationResponse response;
	try{
		fire(client, query, &response);

	}
	catch (...){
		return -1;
	}

	
	try {
		*resp = (char*)calloc (1 , strlen((char*)response.toString().c_str()));
		strncpy(*resp ,  (char*)response.toString().c_str(), strlen((char*)response.toString().c_str()));
	}
	catch (...){
		return -1;
	}


	return 0;

}

int vdb_fire_read_query( void * client,  char * query, char** resp)
{

	voltdb::InvocationResponse response;

	try{
		fire(client, query, &response);
	}
	catch (...){
		return -1;
	}


	try{
		*resp = (char*)calloc (1 , strlen(response.toJSON().c_str()) + 1);
		strncpy(*resp,  response.toJSON().c_str(), strlen(response.toJSON().c_str()));
	}
	catch (...){
		return -1;
	}

	return 0;
}

void  vdb_destroy_client ( void * c )
{
	delete (voltdb::Client*)c;
}

void  vdb_destroy_client_config( void * cc )
{
	delete (voltdb::ClientConfig*)cc;
}


void fire ( void * client,  char * query, voltdb::InvocationResponse* resp)
{
	voltdb::Client* pc = reinterpret_cast<voltdb::Client*>(client);


	std::vector<voltdb::Parameter> parameterTypes(1);
	parameterTypes[0] = voltdb::Parameter(voltdb::WIRE_TYPE_STRING);


	voltdb::Procedure procedure("@AdHoc", parameterTypes);
	procedure.params()->addString(query);
	*resp = pc->invoke(procedure);

}
