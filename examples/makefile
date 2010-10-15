all: helloworld_s helloworld_d

helloworld_s: HelloWorld.cpp libvoltcpp.a
	g++ -D__STDC_CONSTANT_MACROS -D__STDC_LIMIT_MACROS -O3 -arch x86_64 -I./include -pthread HelloWorld.cpp libvoltcpp.a -o helloworld_s
	
helloworld_d: HelloWorld.cpp libvoltcpp.a
	g++ -D__STDC_CONSTANT_MACROS -D__STDC_LIMIT_MACROS -O3 -arch x86_64 -I./include -pthread HelloWorld.cpp libvoltcpp.dylib -o helloworld_d