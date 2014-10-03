VoltDB CPP Client Library
=========================

The VoltDB client library implements the native VoltDB wire protocol. You can
use the library to connect to a VoltDB cluster, invoke stored procedures and
read responses.

When connected to multiple nodes in the cluster, the client library will enable
client affinity and auto-reconnect to restarted nodes.

The Using VoltDB Guide explains how to use the CPP library in the
chapter 14.1.1: Writing VoltDB Client Applications in C++.

Binaries for 64-bit Linux and 64-bit OSX 10.5+ are provided. The linking
instructions below apply to the Linux binaries.

Prerequisites
=============

- Boost C++ Library 1.53.0 or above.

Building
========

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
