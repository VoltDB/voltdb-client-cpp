#!/bin/bash


rm -rf  libbson-1.1.11 
# unpack the bundled libevent tarball, overwriting existing
tar -xzf  libbson-1.1.11.tar.gz

# build libevent binaries with the right configuration
cd  libbson-1.1.11 
./configure  --prefix=`pwd`/../libbsoninstall
make clean
make
make install
cd ..

# remove artifacts we don't care about
# remove the unpacked tarbal
rm -rf  libbson-1.1.11 
rm -rf include/libbson-1.0
rm -rf third_party_libs/linux/libbson-1.0
mkdir third_party_libs/linux/libbson-1.0
mkdir include/libbson-1.0
cp -r libbsoninstall/include/libbson-1.0 include/.
cp -r libbsoninstall/lib/* third_party_libs/linux/libbson-1.0/.
