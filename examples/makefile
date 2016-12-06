VOLT_INCLUDEDIR=..
VOLT_LIBDIR=..
VOLT_LIBS=$(VOLT_LIBDIR)
VOLT_INCLUDES=-I$(VOLT_INCLUDEDIR)/include
CC=g++
RM=rm -rf
CFLAGS=$(BOOST_INCLUDES) $(VOLT_INCLUDES) -g3 -O3

PLATFORM = $(shell uname)
ifeq ($(PLATFORM),Darwin)
    BOOST_INCLUDES=-I/usr/local/include
    BOOST_LIBS=-L/usr/local/lib
    THIRD_PARTY_DIR := $(VOLT_LIBS)/third_party_libs/osx
    THIRD_PARTY_LIBS := $(THIRD_PARTY_DIR)/libevent.a \
                        $(THIRD_PARTY_DIR)/libevent_openssl.a \
                        $(THIRD_PARTY_DIR)/libevent_pthreads.a
    SYSTEM_LIBS := -lpthread $(BOOST_LIBS) -lboost_system-mt -lboost_thread-mt
endif
ifeq ($(PLATFORM),Linux)
    OPENSSL_LIBS=/usr/lib/x86_64-linux-gnu/
	THIRD_PARTY_DIR := $(VOLT_LIBS)/third_party_libs/linux
    THIRD_PARTY_LIBS := $(THIRD_PARTY_DIR)/libevent.a \
                        $(THIRD_PARTY_DIR)/libevent_openssl.a \
                        $(THIRD_PARTY_DIR)/libevent_pthreads.a
    SYSTEM_LIBS := -L$(OPENSSL_LIBS) -lssl -lcrypto -lpthread -lrt -lboost_system-mt -lboost_thread-mt
	#SYSTEM_LIBS := -lpthread -lrt -lboost_system-mt -lboost_thread-mt
endif

all: helloworld asynchelloworld voter svoter

asynchelloworld: AsyncHelloWorld.cpp $(VOLT_LIBS)/libvoltdbcpp.a
	$(CC) $(CFLAGS) AsyncHelloWorld.cpp $(VOLT_LIBS)/libvoltdbcpp.a $(THIRD_PARTY_LIBS) $(SYSTEM_LIBS) -o asynchelloworld

helloworld: HelloWorld.cpp $(VOLT_LIBS)/libvoltdbcpp.a
	$(CC) $(CFLAGS) HelloWorld.cpp $(VOLT_LIBS)/libvoltdbcpp.a $(THIRD_PARTY_LIBS) $(SYSTEM_LIBS) -o helloworld

voter: Voter.cpp $(VOLT_LIBS)/libvoltdbcpp.a
	$(CC) $(CFLAGS) Voter.cpp $(VOLT_LIBS)/libvoltdbcpp.a $(THIRD_PARTY_LIBS) $(SYSTEM_LIBS) -o voter
	
svoter: SVoter.cpp $(VOLT_LIBS)/libvoltdbcpp.a
	$(CC) $(CFLAGS) SVoter.cpp $(VOLT_LIBS)/libvoltdbcpp.a $(THIRD_PARTY_LIBS) $(SYSTEM_LIBS) -o svoter

clean:
	$(RM) asynchelloworld helloworld voter svoter *.dSYM *~
