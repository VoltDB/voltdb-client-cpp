CC=g++
CFLAGS=-I../include -g3 -O3

PLATFORM = $(shell uname)
ifeq ($(PLATFORM),Darwin)
	SYSTEM_LIBS := -lpthread
endif
ifeq ($(PLATFORM),Linux)
	SYSTEM_LIBS := -lpthread -lrt
endif

all: helloworld asynchelloworld voter

asynchelloworld: AsyncHelloWorld.cpp ../libvoltdbcpp.a
	$(CC) $(CFLAGS) AsyncHelloWorld.cpp ../libvoltdbcpp.a ../libevent.a ../libevent_pthreads.a $(SYSTEM_LIBS) -o asynchelloworld

helloworld: HelloWorld.cpp ../libvoltdbcpp.a
	$(CC) $(CFLAGS) HelloWorld.cpp ../libvoltdbcpp.a ../libevent.a ../libevent_pthreads.a $(SYSTEM_LIBS) -o helloworld

voter: Voter.cpp ../libvoltdbcpp.a
	$(CC) $(CFLAGS) Voter.cpp ../libvoltdbcpp.a ../libevent.a ../libevent_pthreads.a $(SYSTEM_LIBS) -o voter

clean:
	rm asynchelloworld helloworld voter
