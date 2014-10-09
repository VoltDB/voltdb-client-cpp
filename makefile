CC=g++
BOOST_INCLUDES=/usr/local/boost
CFLAGS=-I$(BOOST_INCLUDES) -Iinclude -D__STDC_CONSTANT_MACROS -D__STDC_LIMIT_MACROS -g3 -O3 -Wall
LIB_NAME=libvoltdbcpp
KIT_NAME=voltdb-client-cpp-x86_64-3.0

PLATFORM = $(shell uname)
ifeq ($(PLATFORM),Darwin)
	THIRD_PARTY_LIBS := third_party_libs/osx/libevent.a third_party_libs/osx/libevent_pthreads.a
	SYSTEM_LIBS := -lc -lpthread -lboost_system -lboost_thread-mt
endif
ifeq ($(PLATFORM),Linux)
	THIRD_PARTY_LIBS := third_party_libs/linux/libevent.a third_party_libs/linux/libevent_pthreads.a
	SYSTEM_LIBS := -lc -lpthread -lrt -lboost_system -lboost_thread-mt
	CFLAGS += -fPIC
endif

.PHONEY: all clean test kit

OBJS := obj/Client.o \
		obj/ClientConfig.o \
		obj/ClientImpl.o \
		obj/ConnectionPool.o \
		obj/RowBuilder.o \
		obj/sha1.o \
		obj/Table.o \
		obj/WireType.o \
                obj/Distributer.o \
                obj/MurmurHash3.o

TEST_OBJS := test_obj/ByteBufferTest.o \
			 test_obj/MockVoltDB.o \
			 test_obj/ClientTest.o \
			 test_obj/SerializationTest.o \
			 test_obj/Tests.o

CPTEST_OBJS := test_obj/ConnectionPoolTest.o \
			 test_obj/Tests.o

RM := rm -rf

# All Target
all: $(KIT_NAME).tar.gz

obj/%.o: src/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	$(CC) $(CFLAGS) -c -o $@ $?
	@echo 'Finished building: $<'
	@echo ' '

obj/%.o: src/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	$(CC) $(CFLAGS) -c -o $@ $?
	@echo 'Finished building: $<'
	@echo ' '

test_obj/%.o: test_src/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	$(CC) $(CFLAGS) -c -o $@ $?
	@echo 'Finished building: $<'
	@echo ' '

$(LIB_NAME).a: $(OBJS)
	@echo 'Building libvoltdbcpp.a static library'
	$(AR) $(ARFLAGS) $@ $?
	@echo ' '

$(LIB_NAME).so: $(OBJS)
	@echo 'Building libvoltdbcpp.so shared library'
	$(CC) -shared -o $@ $? $(THIRD_PARTY_LIBS) $(SYSTEM_LIBS)
	@echo

$(KIT_NAME).tar.gz: $(LIB_NAME).a $(LIB_NAME).so
	@echo 'Building distribution kit'
	rm -rf $(KIT_NAME)
	mkdir -p $(KIT_NAME)/include/ttmath

	cp -R include/ByteBuffer.hpp include/Client.h include/ClientConfig.h \
		  include/Column.hpp include/ConnectionPool.h include/Decimal.hpp \
		  include/Exception.hpp include/InvocationResponse.hpp include/Parameter.hpp \
		  include/ParameterSet.hpp include/Procedure.hpp include/ProcedureCallback.hpp \
		  include/Row.hpp include/RowBuilder.h include/StatusListener.h include/Table.h \
		  include/TableIterator.h include/WireType.h include/TheHashinator.h \
                  include/ClientLogger.h include/Distributer.h include/ElasticHashinator.h \
                  include/MurmurHash3.h $(KIT_NAME)/include/
	cp -R include/ttmath/*.h $(KIT_NAME)/include/ttmath/
	#cp -R include/boost $(KIT_NAME)/include/

	cp -R examples $(KIT_NAME)/
	cp README.md $(KIT_NAME)/
	cp README.thirdparty $(KIT_NAME)/
	cp $(LIB_NAME).so $(KIT_NAME)/
	cp $(LIB_NAME).a $(KIT_NAME)/
	cp -R $(THIRD_PARTY_LIBS) $(KIT_NAME)/

	tar -czf $(KIT_NAME).tar.gz $(KIT_NAME)

	@echo

testbin: $(LIB_NAME).a $(TEST_OBJS)
	@echo 'Compiling CPPUnit tests'
	$(CC) $(CFLAGS) $(TEST_OBJS) $(LIB_NAME).a $(THIRD_PARTY_LIBS) $(SYSTEM_LIBS) -lcppunit -o testbin
	@echo ' '

test: testbin
	@echo 'Running CPPUnit tests'
	-./testbin
	@echo ' '

# Connection pool testing is put separately since for now it requires a local running Volt server
cptestbin: $(LIB_NAME).a $(CPTEST_OBJS)
	@echo 'Compiling Connection Pool CPPUnit tests'
	$(CC) $(CFLAGS) $(CPTEST_OBJS) $(LIB_NAME).a $(THIRD_PARTY_LIBS) $(SYSTEM_LIBS) -lcppunit -o cptestbin
	@echo ' '

cptest: cptestbin
	@echo 'Running Connection Pool CPPUnit tests'
	-./cptestbin
	@echo ' '

# Other Targets
clean:
	-$(RM) $(OBJS)
	-$(RM) $(TEST_OBJS)
	-$(RM) $(CPTEST_OBJS)
	-$(RM) testbin*
	-$(RM) cptestbin*
	-$(RM) $(LIB_NAME).a
	-$(RM) $(LIB_NAME).so
	-$(RM) $(KIT_NAME)
	-$(RM) $(KIT_NAME).tgz
	-@echo ' '
