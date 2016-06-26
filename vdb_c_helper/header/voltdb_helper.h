#ifndef __VDB_HELPER__
#define __VDB_HELPER__


#ifdef __cplusplus
extern "C" {
#endif



void * vdb_create_client_config( char* uname, char* passwd, unsigned int conn_type );

void * vdb_create_client( void * cc, char* host );

//Supports : C=Create U=Update D=Delete
int vdb_fire_upsert_query( void * client,  char * query, char ** response );

//Supports : R=Read, 50 MB limit to the amount of data volt db returns from a query
int vdb_fire_read_query( void * client,  char * query, char** response );

void  vdb_destroy_client ( void * c );

void  vdb_destroy_client_config( void * cc );


#ifdef __cplusplus
}
#endif

#endif /* __VDB_HELPER__ */
