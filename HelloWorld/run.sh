#!/bin/sh
LD_LIBRARY_PATH=../Library\ Release:../Library\ Debug:${HOME}/lib
export LD_LIBRARY_PATH
cd ../Library\ Debug && make && cd ../HelloWorld && make && HelloWorld
