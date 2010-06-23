#!/bin/sh
cd AsyncHelloWorld && make clean
cd ../HelloWorld && make clean
cd ../Library\ Release && make clean
cd ../Library\ Debug && make clean
cd ../Tests && make clean
