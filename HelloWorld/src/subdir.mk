################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../src/Client.cpp \
../src/HelloWorld.cpp \
../src/RowBuilder.cpp \
../src/Table.cpp \
../src/WireType.cpp 

OBJS += \
./src/Client.o \
./src/HelloWorld.o \
./src/RowBuilder.o \
./src/Table.o \
./src/WireType.o 

CPP_DEPS += \
./src/Client.d \
./src/HelloWorld.d \
./src/RowBuilder.d \
./src/Table.d \
./src/WireType.d 


# Each subdirectory must supply rules for building sources it contributes
src/%.o: ../src/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	g++ -D__STDC_LIMIT_MACROS -DBOOST_SP_DISABLE_THREADS -DDEBUG -I"${HOME}/include" -I../include -O0 -g3 -Wall -Werror -c -fmessage-length=0 -Wno-sign-conversion -Wextra -Wno-unused-parameter -Wno-type-limits -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o"$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


