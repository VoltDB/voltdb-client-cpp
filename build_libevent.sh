#!/bin/bash

# unpack the bundled libevent tarball, overwriting existing
rm -rf libevent-2*-stable
rm -rf libeventinstall
tar -xzf libevent-2*.tar.gz

# build libevent binaries with the right configuration
cd libevent-2*-stable
./configure --disable-shared --with-pic --prefix=`pwd`/../libeventinstall
make clean
make
make install
cd ..

# remove artifacts we don't care about
rm -rf libeventinstall/bin
rm -rf libeventinstall/include/*.h
rm -rf libeventinstall/lib/*.la
rm -rf libeventinstall/lib/libevent_core.a libeventinstall/lib/libevent_extra.a
rm -rf libeventinstall/lib/pkgconfig

# remove the unpacked tarbal
rm -rf libevent-2*-stable
