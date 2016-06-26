#include "stdio.h"
#include "stdlib.h"
#include "voltdb_helper.h"

int main()
{
	int rc = 0;
	void* cc = vdb_create_client_config( (char*)"admin", (char*)"superman", 0);
	//EXPECT_TRUE( NULL != cc);

	void * c = vdb_create_client( cc, (char*)"localhost" );
	if ( NULL == c)
	{
		printf("failed to create client");
		return -1;
	}
	//EXPECT_TRUE( NULL != cc);


	char * qbuf = NULL;
	rc = vdb_fire_upsert_query( c,
	     (char*)"CREATE TABLE Customer (\
             CustomerID INTEGER UNIQUE NOT NULL,\
             FirstName VARCHAR(15),\
             LastName VARCHAR (15),\
             PRIMARY KEY(CustomerID)\
             );", (char**)&qbuf);
	//EXPECT_EQ(0, rc);
	free(qbuf);
	qbuf = NULL;

	rc = vdb_fire_upsert_query( c, (char*)"INSERT INTO Customer VALUES (145303, 'Jane', 'Austin' );",(char**)&qbuf);
	//EXPECT_EQ(0, rc);
	free(qbuf);
	qbuf = NULL;

	rc = vdb_fire_upsert_query( c, (char*)"INSERT INTO Customer VALUES (145304, 'Ruby', 'Austin' );",(char**)&qbuf);
	//EXPECT_EQ(0, rc);
	free(qbuf);
	qbuf = NULL;

	rc = vdb_fire_upsert_query( c, (char*)"INSERT INTO Customer VALUES (145305, 'Peter', 'Austin' );",(char**)&qbuf);
	//EXPECT_EQ(0, rc);
	free(qbuf);
	qbuf = NULL;

	rc = vdb_fire_upsert_query( c, (char*)"INSERT INTO Customer VALUES (145306, 'Peter', 'Austin' );",(char**)&qbuf);
	//EXPECT_EQ(0, rc);
	free(qbuf);
	qbuf = NULL;

	rc = vdb_fire_read_query( c, (char*)"SELECT * from Customer;", (char**)&qbuf );
	printf("The json dump of table : %s", qbuf);
	free(qbuf);
	qbuf = NULL;
	//EXPECT_EQ(0, rc);


	rc = vdb_fire_upsert_query( c, (char*)"DROP TABLE Customer;",  (char**)&qbuf );
	//EXPECT_EQ(0, rc);
	//std::cout << qbuf << std::endl;
	free(qbuf);


	vdb_destroy_client ( c );
	vdb_destroy_client_config( cc );
	return 0;
}
