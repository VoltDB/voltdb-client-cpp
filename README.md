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

API description
===============

The C++ client API is single threaded. The shared pointers that are returned
are not thread safe and the Client instances are not thread safe. Because it is
single threaded your application has to provide its thread to the API to do
network IO and invoke callbacks. At key points your application can call run()
to enter an event loop that will not exit until a callback returns true or one
of the status listener notificaiton methods returns true. An application can
also call runOnce() or spin in a loop on runOnce(). runOnce() does network IO
and invokes callbacks, but returns immediately after all pending events have
been handled.

Unlike the Java client library the C++ library is relatively lightweight
(creates no additional threads) so you can instantiate multiple instances on a
single host. If you find you need more parallelism you should consider creating
multiple threads, each with a dedicated client instance, and connect each
instance to a different subset of the cluster. A single thread should be able
to saturate gig-e.

Some users have implemented a multithreaded wrapper around the volt client without
too much trouble. A useful abstraction is to grab a lock on each client call and
create a locked queue of results (extracted from the ProcedureCallback api) that
can be serviced by a thread pool. All that being said, you need to be careful in
the handling of the data elements in the response since the ProcedureCallback will
be deallocated when the reference count goes to zero.

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
