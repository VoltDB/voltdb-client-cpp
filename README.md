VoltDB CPP Client Library
=========================

The VoltDB client library implements the native VoltDB wire protocol. You can
use the library to connect to a VoltDB cluster, invoke stored procedures and
read responses.

The Using VoltDB Guide explains how to use the CPP library in
[Chapter 15.1: C++ Client Interface.]
(http://docs.voltdb.com/UsingVoltDB/ChapOtherClientAPI.php)

Binaries for 64-bit Linux and 64-bit OSX 10.5+ are provided. The linking
instructions below apply to the Linux binaries.

New Features in V5.2
==================

You can now use SHA-256 to hash password for authentication request. V5.2 onward
server is required. The default is SHA-1. To use SHA-256 create client config as
```C++
voltdb::ClientConfig config("myusername", "mypassword", voltdb::HASH_SHA256);</code>
voltdb::Client client = voltdb::Client::create(config);
```

New Features in V4
==================

When connected to multiple nodes in the cluster, the client library will enable
client affinity and can auto-reconnect to restarted nodes.

Prerequisites
=============

- Boost C++ Library 1.53.0 or above.

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
