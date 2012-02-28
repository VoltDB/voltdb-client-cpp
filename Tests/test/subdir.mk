################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../test/ByteBufferTest.cpp \
../test/ClientTest.cpp \
../test/MockVoltDB.cpp \
../test/SerializationTest.cpp \
../test/Tests.cpp 

OBJS += \
./test/ByteBufferTest.o \
./test/ClientTest.o \
./test/MockVoltDB.o \
./test/SerializationTest.o \
./test/Tests.o 

CPP_DEPS += \
./test/ByteBufferTest.d \
./test/ClientTest.d \
./test/MockVoltDB.d \
./test/SerializationTest.d \
./test/Tests.d 


# Each subdirectory must supply rules for building sources it contributes
test/%.o: ../test/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	g++ -D__STDC_LIMIT_MACROS -DDEBUG -I"${HOME}/include" -I../include -I../libeventinstall/include -O0 -g3 -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o"$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


