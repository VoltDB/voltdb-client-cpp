#!/bin/sh
cd libevent
./configure --disable-shared --disable-openssl --with-pic --prefix=`pwd`/../libeventinstall
make clean
make
make install
cd ..
rm -fr *.a *.tgz tmp package
cd library_release
make clean
make
cd ..
cd library_release_static
make clean
make
cd ..
mkdir tmp
cd tmp
mkdir volt && cd volt
echo `pwd`
ar x ../../library_release_static/libvoltcpp.a
cd .. && mkdir event && cd event
echo `pwd`
ar x ../../libeventinstall/lib/libevent.a
cd ../..
echo `pwd`
ar rcs libvoltcpp.a tmp/volt/* tmp/event/*

mkdir package
mkdir package/include
mkdir package/include/ttmath
cp library_release/libvoltcpp.so package
cd include
cp -r ByteBuffer.hpp Client.h ClientConfig.h Column.hpp ConnectionPool.h Decimal.hpp Exception.hpp InvocationResponse.hpp Parameter.hpp ParameterSet.hpp Procedure.hpp ProcedureCallback.hpp Row.hpp RowBuilder.h StatusListener.h Table.h TableIterator.h WireType.h ../package/include
cp -r ttmath/*.h ../package/include/ttmath
cp -r boost ../package/include
cd ..
cp libvoltcpp.a package
cd package
tar cvfz ../volt_cpp.tgz *
strip -d --strip-unneeded libvoltcpp.a
strip -d --strip-unneeded libvoltcpp.so
tar cvfz ../volt_cpp_stripped.tgz *
cd ..
rm -fr tmp
