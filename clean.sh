#!/bin/sh
rm -fr package tmp libeventinstall *.a *.tgz
cd library_release && make clean
cd ../library_debug && make clean
cd ../library_release_static && make clean
cd ../Tests && make clean
cd ../libevent && make clean
