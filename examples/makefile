VOLT_INCLUDEDIR=..
VOLT_LIBDIR=..
VOLT_LIBS=$(VOLT_LIBDIR)
VOLT_INCLUDES=-I$(VOLT_INCLUDEDIR)/include
RM=rm -rf
CXXFLAGS=$(VOLT_INCLUDES) -std=c++11 -g3 -O3 -fPIC

PLATFORM = $(shell uname)
ifeq ($(PLATFORM),Darwin)
	  THIRD_PARTY_DIR := $(VOLT_LIBS)/third_party_libs/osx
	  THIRD_PARTY_LIBS := $(THIRD_PARTY_DIR)/libevent.a \
		    $(THIRD_PARTY_DIR)/libevent_openssl.a \
		    $(THIRD_PARTY_DIR)/libevent_pthreads.a \
		    $(THIRD_PARTY_DIR)/libssl.a \
		    $(THIRD_PARTY_DIR)/libcrypto.a
	  SYSTEM_LIBS := -lpthread
else ifeq ($(PLATFORM),Linux)
	  THIRD_PARTY_DIR := $(VOLT_LIBS)/third_party_libs/linux
	  THIRD_PARTY_LIBS := $(THIRD_PARTY_DIR)/libevent.a \
		    $(THIRD_PARTY_DIR)/libevent_openssl.a \
		    $(THIRD_PARTY_DIR)/libevent_pthreads.a \
		    $(THIRD_PARTY_DIR)/libssl.a \
		    $(THIRD_PARTY_DIR)/libcrypto.a \
		    -ldl
	  SYSTEM_LIBS := -lpthread -lrt
else
	  $(error "Unsupported platform $(PLATFORM)")
endif

all: helloworld asynchelloworld voter sslvoter

asynchelloworld: AsyncHelloWorld.cpp $(VOLT_LIBS)/libvoltdbcpp.a
	$(CXX) $(CXXFLAGS) AsyncHelloWorld.cpp $(VOLT_LIBS)/libvoltdbcpp.a $(THIRD_PARTY_LIBS) $(SYSTEM_LIBS) -o asynchelloworld

helloworld: HelloWorld.cpp $(VOLT_LIBS)/libvoltdbcpp.a
	$(CXX) $(CXXFLAGS) HelloWorld.cpp $(VOLT_LIBS)/libvoltdbcpp.a $(THIRD_PARTY_LIBS) $(SYSTEM_LIBS) -o helloworld

voter: Voter.cpp $(VOLT_LIBS)/libvoltdbcpp.a
	$(CXX) $(CXXFLAGS) Voter.cpp $(VOLT_LIBS)/libvoltdbcpp.a $(THIRD_PARTY_LIBS) $(SYSTEM_LIBS) -o voter

sslvoter: SSLVoter.cpp $(VOLT_LIBS)/libvoltdbcpp.a
	$(CXX) $(CXXFLAGS) SSLVoter.cpp $(VOLT_LIBS)/libvoltdbcpp.a $(THIRD_PARTY_LIBS) $(SYSTEM_LIBS) -o sslvoter

clean:
	$(RM) asynchelloworld helloworld voter sslvoter *.dSYM *~

