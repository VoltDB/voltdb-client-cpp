################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../src/ByteBufferTest.cpp \
../src/Client.cpp \
../src/RowBuilder.cpp \
../src/Table.cpp 

OBJS += \
./src/ByteBufferTest.o \
./src/Client.o \
./src/RowBuilder.o \
./src/Table.o 

CPP_DEPS += \
./src/ByteBufferTest.d \
./src/Client.d \
./src/RowBuilder.d \
./src/Table.d 


# Each subdirectory must supply rules for building sources it contributes
src/%.o: ../src/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	g++ -D__STDC_LIMIT_MACROS -I"/home/aweisberg/cpp_api/trunk/include" -I/home/aweisberg/include -O0 -g3 -Wall -Werror -c -fmessage-length=0 -Wsign-conversion -Wextra -Wno-unused-parameter -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o"$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


