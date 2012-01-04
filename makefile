CC=g++
CFLAGS=-Iinclude -D__STDC_CONSTANT_MACROS -D__STDC_LIMIT_MACROS -g3 -O2

PLATFORM = $(shell uname)
ifeq ($(PLATFORM),Darwin)
	THIRD_PARTY_LIBS := third_party_libs/osx/libevent.a third_party_libs/osx/libevent_pthreads.a
	SYSTEM_LIBS := -lpthread
endif
ifeq ($(PLATFORM),Linux)
	THIRD_PARTY_LIBS := third_party_libs/linux/libevent.a third_party_libs/linux/libevent_pthreads.a
	SYSTEM_LIBS := -lpthread -lrt
	CFLAGS += -fPIC
endif

.PHONEY: all clean test

OBJS := obj/CoreClient.o \
        obj/RowBuilder.o \
        obj/sha1.o \
        obj/Table.o \
        obj/WireType.o

TEST_OBJS := test_obj/ByteBufferTest.o \
             test_obj/SerializationTest.o \
             test_obj/Tests.o

RM := rm -rf

# All Target
all: libvoltdbcpp.a

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

libvoltdbcpp.a: $(OBJS)
	@echo 'Building libvoltdbcpp.a library'
	$(AR) $(ARFLAGS) $@ $(OBJS)
	@echo ' '

example: libvoltdbcpp.a
	@echo 'Compiling and running simple example'
	$(CC) $(CFLAGS) example.cpp libvoltdbcpp.a $(THIRD_PARTY_LIBS) $(SYSTEM_LIBS) -o example
	./example
	@echo ' '

testbin: libvoltdbcpp.a $(TEST_OBJS)
	@echo 'Compiling CPPUnit tests'
	$(CC) $(CFLAGS) $(TEST_OBJS) libvoltdbcpp.a $(THIRD_PARTY_LIBS) $(SYSTEM_LIBS) -lcppunit -o testbin
	@echo ' '

test: testbin
	@echo 'Running CPPUnit tests'
	-./testbin
	@echo ' '

# Other Targets
clean:
	-$(RM) $(OBJS)
	-$(RM) $(TEST_OBJS)
	-$(RM) testbin*
	-$(RM) libvoltdbcpp.a
	-$(RM) example example.d*
	-@echo ' '
