#!/bin/sh
rm -fr package tmp *.a *.tgz
cd AsyncHelloWorld && make clean
cd ../HelloWorld && make clean
cd ../library_release && make clean
cd ../library_debug && make clean
cd ../library_release_static && make clean
cd ../Tests && make clean
