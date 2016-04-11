CC=g++
BOOST_INCLUDES=/usr/local/boost
CFLAGS=-I$(BOOST_INCLUDES) -I../include -g3 -O3

PLATFORM = $(shell uname)
ifeq ($(PLATFORM),Darwin)
    THIRD_PARTY_LIBS := ../third_party_libs/osx/libevent.a ../third_party_libs/osx/libevent_pthreads.a
    SYSTEM_LIBS := -lpthread -lboost_system-mt -lboost_thread-mt
endif
ifeq ($(PLATFORM),Linux)
    THIRD_PARTY_LIBS := ../third_party_libs/linux/libevent.a ../third_party_libs/linux/libevent_pthreads.a
    SYSTEM_LIBS := -lpthread -lrt -lboost_system -lboost_thread
endif

all: helloworld asynchelloworld voter

asynchelloworld: AsyncHelloWorld.cpp ../libvoltdbcpp.a
	$(CC) $(CFLAGS) AsyncHelloWorld.cpp ../libvoltdbcpp.a $(THIRD_PARTY_LIBS) $(SYSTEM_LIBS) -o asynchelloworld

helloworld: HelloWorld.cpp ../libvoltdbcpp.a
	$(CC) $(CFLAGS) HelloWorld.cpp ../libvoltdbcpp.a $(THIRD_PARTY_LIBS) $(SYSTEM_LIBS) -o helloworld

voter: Voter.cpp ../libvoltdbcpp.a
	$(CC) $(CFLAGS) Voter.cpp ../libvoltdbcpp.a $(THIRD_PARTY_LIBS) $(SYSTEM_LIBS) -o voter

clean:
	rm -f asynchelloworld helloworld voter
