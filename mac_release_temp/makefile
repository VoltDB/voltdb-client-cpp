# warnings
CPPFLAGS += -Wall -fmessage-length=0
# weird crap
CPPFLAGS += -D__STDC_CONSTANT_MACROS -D__STDC_LIMIT_MACROS
# code output
CPPFLAGS += -O3 -arch x86_64
# headers
CPPFLAGS += -I../include
# libraries
CPPFLAGS += -pthread

LDFLAGS += -shared -dynamic -arch x86_64 -lpthread

# Add inputs and outputs from these tool invocations to the build variables
CPP_SRCS += \
../src/Client.cpp \
../src/ClientConfig.cpp \
../src/ClientImpl.cpp \
../src/ConnectionPool.cpp \
../src/RowBuilder.cpp \
../src/Table.cpp \
../src/WireType.cpp

C_SRCS += \
../src/sha1.c

OBJS += \
./src/Client.o \
./src/ClientConfig.o \
./src/ClientImpl.o \
./src/ConnectionPool.o \
./src/RowBuilder.o \
./src/Table.o \
./src/WireType.o \
./src/sha1.o

USER_OBJS := /usr/local/lib/libevent.a /usr/local/lib/libboost_thread.a

RM := rm

# All Target
all: libvoltcpp.dylib libvoltcpp.a

# Each subdirectory must supply rules for building sources it contributes
src/%.o: ../src/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	$(CCACHE) $(COMPILE.cpp) -O3 -c -o $@ $?
	@echo 'Finished building: $<'
	@echo ' '

src/%.o: ../src/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	$(CCACHE) $(COMPILE.cpp) -c -o $@ $?
	@echo 'Finished building: $<'
	@echo ' '

# Tool invocations
libvoltcpp.a: $(OBJS) $(USER_OBJS)
	@echo 'Building target: $@'
	@echo 'Invoking: GCC C++ Linker'
	$(AR) $(ARFLAGS) $@ $?
	@echo 'Finished building target: $@'
	@echo ' '

# Tool invocations
libvoltcpp.dylib: $(OBJS) $(USER_OBJS)
	@echo 'Building target: $@'
	@echo 'Invoking: GCC C++ Linker'
	g++ $(LDFLAGS) -o $@ $^
	@echo 'Finished building target: $@'
	@echo ' '

# Other Targets
clean:
	-$(RM) $(OBJS)$(LIBRARIES) libvoltcpp.dylib libvoltcpp.a
	-@echo ' '

.PHONY: all clean dependents