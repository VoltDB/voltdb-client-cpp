#
# Set this to DEBUG on the command line if you want to build
# with debugging.
#
BUILD=RELEASE
ifeq (${BUILD},RELEASE)
	  OPTIMIZATION=-O3
else
	  OPTIMIZATION=-g3
endif

LIB_NAME=libvoltdbcpp
KIT_NAME=voltdb-client-cpp-x86_64-8.4

CXXFLAGS=-Iinclude -std=c++11 ${OPTIMIZATION} -fPIC
PLATFORM = $(shell uname)


ifeq ($(PLATFORM),Darwin)
	  THIRD_PARTY_DIR := third_party_libs/osx
	  THIRD_PARTY_LIBS := $(THIRD_PARTY_DIR)/libevent.a \
		    $(THIRD_PARTY_DIR)/libevent_openssl.a \
		    $(THIRD_PARTY_DIR)/libevent_pthreads.a \
		    $(THIRD_PARTY_DIR)/libssl.a \
		    $(THIRD_PARTY_DIR)/libcrypto.a
	  SYSTEM_LIBS := -lc -lpthread
else ifeq ($(PLATFORM),Linux)
	  THIRD_PARTY_DIR := third_party_libs/linux
	  THIRD_PARTY_LIBS := $(THIRD_PARTY_DIR)/libevent.a \
		    $(THIRD_PARTY_DIR)/libevent_openssl.a \
		    $(THIRD_PARTY_DIR)/libevent_pthreads.a \
		    $(THIRD_PARTY_DIR)/libssl.a \
		    $(THIRD_PARTY_DIR)/libcrypto.a \
		    -ldl
	  SYSTEM_LIBS := -lc -lpthread -lrt
else
	  $(error "Unsupported platform $(PLATFORM)")
endif

.PHONEY: all clean test kit

OBJS := obj/Client.o \
		obj/ClientConfig.o \
		obj/ClientImpl.o \
		obj/ConnectionPool.o \
		obj/RowBuilder.o \
		obj/Table.o \
		obj/WireType.o \
		obj/Distributer.o \
		obj/MurmurHash3.o \
		obj/GeographyPoint.o \
		obj/Geography.o

TEST_OBJS := test_obj/ByteBufferTest.o \
			 test_obj/MockVoltDB.o \
			 test_obj/ClientTest.o \
			 test_obj/SerializationTest.o \
			 test_obj/GeographyPointTest.o \
			 test_obj/GeographyTest.o \
			 test_obj/TableTest.o \
			 test_obj/Tests.o

CPTEST_OBJS := test_obj/ConnectionPoolTest.o \
			 test_obj/Tests.o

RM := $(RM) -r

# All Target
all: $(KIT_NAME).tar.gz

obj/%.o: src/%.cpp | obj
	$(CXX) $(CXXFLAGS) -c -o $@ $< -MMD -MP

obj/%.o: src/%.c | obj
	$(CXX) $(CXXFLAGS) -c -o $@ $< -MMD -MP

test_obj/%.o: test_src/%.cpp | test_obj
	$(CXX) $(CXXFLAGS) -c -o $@ $< -MMD -MP

$(LIB_NAME).a: $(OBJS)
	@echo 'Building libvoltdbcpp.a static library'
	$(AR) $(ARFLAGS) $@ $?

$(LIB_NAME).so: $(OBJS)
	@echo 'Building libvoltdbcpp.so shared library'
	$(CXX) $(CXXFLAGS) -shared -o $@ $? $(THIRD_PARTY_LIBS) $(SYSTEM_LIBS)

-include $(OBJS:.o=.d)
-include $(TEST_OBJS:.o=.d)

$(KIT_NAME).tar.gz: $(LIB_NAME).a $(LIB_NAME).so
	@echo 'Building distribution kit'
	rm -rf $(KIT_NAME)
	mkdir -p $(KIT_NAME)/include/ttmath
	mkdir -p $(KIT_NAME)/include/openssl
	mkdir -p $(KIT_NAME)/$(THIRD_PARTY_DIR)

	cp -R include/ByteBuffer.hpp include/Client.h include/ClientConfig.h \
		  include/Column.hpp include/ConnectionPool.h include/Decimal.hpp \
		  include/Exception.hpp include/InvocationResponse.hpp include/Parameter.hpp \
		  include/ParameterSet.hpp include/Procedure.hpp include/ProcedureCallback.hpp \
		  include/Row.hpp include/RowBuilder.h include/StatusListener.h include/Table.h \
		  include/TableIterator.h include/WireType.h include/TheHashinator.h \
		  include/ClientLogger.h include/Distributer.h include/ElasticHashinator.h \
		  include/MurmurHash3.h include/Geography.hpp include/GeographyPoint.hpp $(KIT_NAME)/include/
	cp -R include/ttmath/*.h $(KIT_NAME)/include/ttmath/
	cp include/openssl/*.h $(KIT_NAME)/include/openssl/
	cp include/property_tree/*.hpp $(KIT_NAME)/include/property_tree

	cp -R examples $(KIT_NAME)/
	cp README.md $(KIT_NAME)/
	cp README.thirdparty $(KIT_NAME)/
	cp $(LIB_NAME).so $(KIT_NAME)/
	cp $(LIB_NAME).a $(KIT_NAME)/
	cp -pR $(THIRD_PARTY_LIBS) $(KIT_NAME)/$(THIRD_PARTY_DIR)

	tar -czf $(KIT_NAME).tar.gz $(KIT_NAME)

testbin: $(LIB_NAME).a $(TEST_OBJS)
	@echo 'Compiling CPPUnit tests'
	$(CXX) $(CXXFLAGS) $(TEST_OBJS) $(LIB_NAME).a $(THIRD_PARTY_LIBS) $(SYSTEM_LIBS) -lcppunit -o testbin

test: testbin
	@echo 'Running CPPUnit tests'
	./testbin

# Connection pool testing is put separately since for now it requires a local running Volt server
cptestbin: $(LIB_NAME).a $(CPTEST_OBJS)
	@echo 'Compiling Connection Pool CPPUnit tests'
	$(CXX) $(CXXFLAGS) $(CPTEST_OBJS) $(LIB_NAME).a $(THIRD_PARTY_LIBS) $(SYSTEM_LIBS) -lcppunit -o cptestbin

cptest: cptestbin
	@echo 'Running Connection Pool CPPUnit tests'
	./cptestbin

obj:
	mkdir -p obj

test_obj:
	mkdir -p test_obj

# Other Targets
clean:
	-$(RM) $(OBJS) $(OBJS:.o=.d) $(TEST_OBJS) $(TEST_OBJS:.o=.d) $(CPTEST_OBJS) testbin* cptestbin* \
		  $(LIB_NAME).a $(LIB_NAME).so $(KIT_NAME) $(KIT_NAME).tgz obj test_obj
	$(MAKE) -C examples clean
# DO NOT DELETE
