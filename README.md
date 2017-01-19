VoltDB CPP Client Library
=========================

The VoltDB client library implements the native VoltDB wire protocol. You can
use the library to connect to a VoltDB cluster, invoke stored procedures and
read responses.

The Using VoltDB Guide explains how to use the CPP library in
[Chapter 15.1: C++ Client Interface.]
(http://docs.voltdb.com/UsingVoltDB/ChapOtherClientAPI.php)

Binaries for 64-bit Linux and 64-bit OSX are provided. The linking
instructions below apply to the Linux binaries.

The Linux binary was compiled with GCC 4.4.7 on Centos 6.7.
The OSX binary was compiled with Xcode 7.2 on OSX 10.11.

The source code is available in the [VoltDB Github repository]
(https://github.com/VoltDB/voltdb-client-cpp). 

New Features in 7.0
==================
- Support for Table (synomous to VoltTable on server side) in parameter set.
How to construct table: initialize table with column schema. Generate row to be inserted - initialize the row with same
column schema as that of table, populate the row and add the row to the table. After inserting the populated row to the
table, the row instance can be reused to populate it row with new values and push it table.
```C++
std::vector<voltdb::Column> columns;
columns.push_back(voltdb::Column("col1", voltdb::WIRE_TYPE_BIGINT));
columns.push_back(voltdb::Column("col2", voltdb::WIRE_TYPE_STRING));

voltdb::Table table (columns);
voltdb::RowBuilder row(columns);
row.addInt64(1234).addString("hello");
table.addRow(row);
row.addNull().addString("unknown");
table.addRow(row);
```

New Features in V6.9
==================
- Support for timing out outstanding/pending invocation requests. By default the monitoring of the pending requests for timeout is disabled. To exercise feature, timeout has to be enabled using client config. Default timeout is 10 seconds and can be tweaked through client config.
```C++
bool enableQueryTimeout = true;
int queryTimeout = 10;
voltdb::ClientConfig config("myusername", "mypassword", voltdb::HASH_SHA256, true, enableQueryTimeout, queryTimeout);
voltdb::Client client = voltdb::Client::create(config);
```


New Features in V6.8
==================
- Update backpressure notification to notify when backpressure on client transitions from on to off
- Fix handling of pending connection logic where client gets disconnected and tries to reconnect, if socket timeout is longer than 10 seconds, it can potentially crash the client
- Update setaffinity logic to not drop the set affinity request in case of backpresure with client configured with enable abandon set to true.

New Features in V6.0
==================

Support for new geospatial datatypes: GEOGRAPHY and GEOGRAPHY_POINT. The client can fetch
columns using getGeography() and getGeographyPoint().

New Features in V5.2
==================

You can now use SHA-256 to hash password for authentication request. V5.2 onward
server is required. The default is SHA-1. To use SHA-256 create client config as
```C++
voltdb::ClientConfig config("myusername", "mypassword", voltdb::HASH_SHA256);
voltdb::Client client = voltdb::Client::create(config);
```

New Features in V4
==================

When connected to multiple nodes in the cluster, the client library will enable
client affinity and can auto-reconnect to restarted nodes.

Prerequisites
=============

Boost C++ Library 1.53 if using the downloaded client library. If building from
source, you may use 1.53 or above.

Building
========
*This does not apply if you have downloaded the kit from voltdb.com*

Simply type `make` in the unpacked directory to build the client library. It
will generate a shared library and a static library for linking your program
against.

Building the kit samples
========================

In terminal go to the examples directory then run:
```bash
make
```

This will build the HelloWorld, AsyncHelloWorld and Voter applications
that you can then run against a locally running HelloWorld or Voter database.

You can additionally view the source code of the c++ client library and the
examples online at:

      https://github.com/VoltDB/voltdb-client-cpp

Linking against libvoltdbcpp.a
==============================
*Only applies for linux distribution*

The following command will compile and link an example client
(clientvoter.cpp) libvoltdb.a.

Note that -lrt and -pthread must appear after the object files being compiled/linked
in more recent Linux distributions.

Directory structure for this example:
```
./clientvoter.cpp        # Example client
./include                # Directory containing client library headers
./                       # Directory containing client library archive
```

```bash
g++ -I./include/                 \
clientvoter.cpp                  \
./libvoltdbcpp.a                 \
./libevent.a                     \
./libevent_pthread.a             \
-lrt                             \
-pthread                         \
-lboost_system                   \
-lboost_thread                   \
-o voter
```

Note that -lrt should not be included for the mac edition. See the example makefile
for more detail.
