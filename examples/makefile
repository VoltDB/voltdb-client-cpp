VOLT_INCLUDEDIR=..
VOLT_LIBDIR=..
VOLT_LIBS=$(VOLT_LIBDIR)
VOLT_INCLUDES=-I$(VOLT_INCLUDEDIR)/include
CC=g++
RM=rm -rf
OPTIMIZATION=-O3
CFLAGS=$(BOOST_INCLUDES) $(VOLT_INCLUDES) -g3 ${OPTIMIZATION}
CFLAGS += -fPIC

PLATFORM = $(shell uname)
ifeq ($(PLATFORM),Darwin)
    BOOST_INCLUDES=-I/usr/local/include
    BOOST_LIBS=-L/usr/local/lib
    SYSTEM_LIBS := -lpthread $(BOOST_LIBS) -lboost_system-mt -lboost_thread-mt
    THIRD_PARTY_DIR := $(VOLT_LIBS)/third_party_libs/osx
    THIRD_PARTY_LIBS := $(THIRD_PARTY_DIR)/libevent.a \
                        $(THIRD_PARTY_DIR)/libevent_openssl.a \
                        $(THIRD_PARTY_DIR)/libevent_pthreads.a \
                        $(THIRD_PARTY_DIR)/libssl.a \
                        $(THIRD_PARTY_DIR)/libcrypto.a
endif
ifeq ($(PLATFORM),Linux)
    THIRD_PARTY_DIR := $(VOLT_LIBS)/third_party_libs/linux
    THIRD_PARTY_LIBS := $(THIRD_PARTY_DIR)/libevent.a \
                        $(THIRD_PARTY_DIR)/libevent_openssl.a \
                        $(THIRD_PARTY_DIR)/libevent_pthreads.a \
                        $(THIRD_PARTY_DIR)/libssl.a \
                        $(THIRD_PARTY_DIR)/libcrypto.a \
                        -ldl
    SYSTEM_LIBS := -lpthread -lrt -lboost_system -lboost_thread
endif

all: helloworld asynchelloworld voter sslvoter

asynchelloworld: AsyncHelloWorld.cpp $(VOLT_LIBS)/libvoltdbcpp.a
	$(CC) $(CFLAGS) AsyncHelloWorld.cpp $(VOLT_LIBS)/libvoltdbcpp.a $(THIRD_PARTY_LIBS) $(SYSTEM_LIBS) -o asynchelloworld

helloworld: HelloWorld.cpp $(VOLT_LIBS)/libvoltdbcpp.a
	$(CC) $(CFLAGS) HelloWorld.cpp $(VOLT_LIBS)/libvoltdbcpp.a $(THIRD_PARTY_LIBS) $(SYSTEM_LIBS) -o helloworld

voter: Voter.cpp $(VOLT_LIBS)/libvoltdbcpp.a
	$(CC) $(CFLAGS) Voter.cpp $(VOLT_LIBS)/libvoltdbcpp.a $(THIRD_PARTY_LIBS) $(SYSTEM_LIBS) -o voter

sslvoter: SSLVoter.cpp $(VOLT_LIBS)/libvoltdbcpp.a
	$(CC) $(CFLAGS) SSLVoter.cpp $(VOLT_LIBS)/libvoltdbcpp.a $(THIRD_PARTY_LIBS) $(SYSTEM_LIBS) -o sslvoter

clean:
	$(RM) asynchelloworld helloworld voter sslvoter *.dSYM *~
