#!/bin/sh
rm -fr *.a *.gz tmp package
cd library_release_static
make clean
make
cd ..
mkdir tmp
cd tmp
mkdir volt && cd volt
ar x ../../library_release_static/libvoltcpp.a
cd .. && mkdir event && cd event
ar x $HOME/lib/libevent.a
cd .. && mkdir boost_thread && cd boost_thread
ar x $HOME/lib/libboost_thread.a
cd .. && mkdir boost_system && cd boost_system
ar x $HOME/lib/libboost_system.a
cd .. && mkdir boost_filesystem && cd boost_filesystem
ar x $HOME/lib/libboost_filesystem.a
cd ../..
ar rcs libvoltcpp.a tmp/volt/* tmp/event/* tmp/boost_system/* tmp/boost_thread/* tmp/boost_filesystem/* tmp/boost_thread/*

mkdir package
mkdir package/include
cd include
cp -r ByteBuffer.hpp Client.h ClientConfig.h Column.hpp ConnectionPool.h Decimal.hpp Exception.hpp InvocationResponse.hpp Parameter.hpp ParameterSet.hpp Procedure.hpp ProcedureCallback.hpp Row.hpp RowBuilder.h StatusListener.h Table.h TableIterator.h WireType.h ttmath ../package/include
cd ..
cp libvoltcpp.a package
cd package
tar cvfz ../volt_cpp.tgz *
strip -d --strip-unneeded libvoltcpp.a
tar cvfz ../volt_cpp_stripped.tgz *
cd ..
rm -fr tmp
