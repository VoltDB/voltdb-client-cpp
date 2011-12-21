CC=g++
CFLAGS=-Iinclude

PLATFORM = $(shell uname)
ifeq ($(PLATFORM),Darwin)
	THIRD_PARTY_LIBS := third_party_libs/osx/libevent.a third_party_libs/osx/libevent_pthreads.a
endif
ifeq ($(PLATFORM),Linux)
	THIRD_PARTY_LIBS := third_party_libs/linux/libevent.a third_party_libs/linux/libevent_pthreads.a
endif

.PHONEY: all clean test

OBJS := obj/Client.o obj/RowBuilder.o obj/sha1.o obj/Table.o obj/WireType.o

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

libvoltdbcpp.a: $(OBJS)
	@echo 'Building libvoltdbcpp.a library'
	$(AR) $(ARFLAGS) $@ $(OBJS)
	@echo ' '

test: libvoltdbcpp.a
	@echo 'Compiling and running simple example'
	$(CC) $(CFLAGS) main.cpp libvoltdbcpp.a $(THIRD_PARTY_LIBS) -o main
	./main
	@echo ' '

# Other Targets
clean:
	-$(RM) $(OBJS)
	-$(RM) libvoltdbcpp.a
	-$(RM) make
	-@echo ' '
