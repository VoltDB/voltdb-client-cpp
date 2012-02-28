#!/bin/bash -e

# Build script for kit deployment

# Remove any old build
rm -rf HelloWorld
rm -rf AsyncHelloWorld

# Figure out where the library is located (different for Linux and Mac)
lib=`ls -d ../lib_*`

# Build the samples
echo "Building 'HellowWorld' application"
g++ -lrt -I../include/ -D__STDC_LIMIT_MACROS HelloWorld.cpp $lib/libvoltcpp.a -o HelloWorld

echo "Building 'AsyncHellowWorld' application"
g++ -lrt -I../include/ -D__STDC_LIMIT_MACROS AsyncHelloWorld.cpp $lib/libvoltcpp.a -o AsyncHelloWorld

echo "Building 'Voter' application"
g++ -lrt -I../include/ -D__STDC_LIMIT_MACROS Voter.cpp $lib/libvoltcpp.a -o Voter

echo "Build complete.  Run ./HelloWorld or ./AsyncHelloWorld against a running HelloWorld database on localhost."
