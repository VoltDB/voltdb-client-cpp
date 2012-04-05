CC=g++
CFLAGS=-I../include -D__STDC_CONSTANT_MACROS -D__STDC_LIMIT_MACROS -g3 -O3

PLATFORM = $(shell uname)
ifeq ($(PLATFORM),Darwin)
	SYSTEM_LIBS := -lpthread
endif
ifeq ($(PLATFORM),Linux)
	SYSTEM_LIBS := -lpthread -lrt
endif

all: helloworld asynchelloworld voter

asynchelloworld: AsyncHelloWorld.cpp ../libvoltcpp.a
	$(CC) $(CFLAGS) HelloWorld.cpp ../libvoltcpp.a ../libevent.a ../libevent_pthreads.a $(SYSTEM_LIBS) -o asynchelloworld

helloworld: HelloWorld.cpp ../libvoltcpp.a
	$(CC) $(CFLAGS) HelloWorld.cpp ../libvoltcpp.a ../libevent.a ../libevent_pthreads.a $(SYSTEM_LIBS) -o helloworld

voter: Voter.cpp ../libvoltcpp.a
	$(CC) $(CFLAGS) Voter.cpp ../libvoltcpp.a ../libevent.a ../libevent_pthreads.a $(SYSTEM_LIBS) -o voter

clean:
	rm asynchelloworld helloworld voter
