include mk/init.make

TARGETS:=VoltClientLib VoltClient_utest

VoltClientLib_LIBDEPS:=c pthread rt event event_pthreads \
			openssl \
                        boost_thread 

VoltClientLib_DEFINES:=-D__STDC_CONSTANT_MACROS -D__STDC_LIMIT_MACROS
VoltClientLib_INCLUDES:=VoltClient/include

VoltClientLib_SRCS:=src/Client.cpp \
				src/ClientConfig.cpp \
				src/ClientImpl.cpp \
				src/ConnectionPool.cpp \
				src/RowBuilder.cpp \
				src/Table.cpp \
				src/WireType.cpp\
				src/Distributer.cpp\
				src/MurmurHash3.cpp

				

VoltClientLib_TARGET_NAME:=voltdbcpp
VoltClientLib_TARGET_TYPE:=shlib


# disable warnings-as-errors     -Wtype-limits is enable by -Wextra
##http://www.ttmath.org/issue/patch__fix_warning__suggest_a_space_before_____or_explicit_braces_around_empty_body_in__for__statement
ifeq ($(_USE_GCC),T)
VoltClientLib_CXXFLAGS:=-Wno-error -Wall
endif

$(call addonsInstallerForVolt,VoltClientLib,,../typedocs/Volt.bundle,no_dsdhelp)

#Unit tests
VoltClient_utest_TARGET_NAME:=gtest_volt

VoltClient_utest_TARGET_TYPE:=gtest
# Moved the copy of the binary test files to here instead, 
# so they are called every time the gtest target is invoked.
VoltClient_utest_TEST_SETUP:=gtestSetup.sh

VoltClient_utest_LIBDEPS:=voltclient \
			gtest \
			gmock \
			gtest_main \
			event \
			event_pthreads \
                        boost_thread \
                        boost_system \
                        boost_chrono pthread

VoltClient_utest_DEFINES+=-D__STDC_CONSTANT_MACROS -D__STDC_LIMIT_MACROS

VoltClient_utest_INCLUDES:=VoltClient/include \
			VoltClient/test_src/ \
			./

VoltClient_utest_SRCS:= gtest/Client_tests.cc \
                    test_src/MockVoltDB.cpp

include mk/make.make

