all: helloworld asynchelloworld voter

asynchelloworld: AsyncHelloWorld.cpp ../libvoltcpp.a
	g++ -D__STDC_CONSTANT_MACROS -D__STDC_LIMIT_MACROS -O0 -g3 -I../include -lrt -pthread HelloWorld.cpp ../libvoltcpp.a -lrt -o asynchelloworld
	
helloworld: HelloWorld.cpp ../libvoltcpp.a
	g++ -D__STDC_CONSTANT_MACROS -D__STDC_LIMIT_MACROS -O0 -g3 -I../include -lrt -pthread HelloWorld.cpp ../libvoltcpp.a -lrt -o helloworld

voter: Voter.cpp ../libvoltcpp.a
	g++ -D__STDC_CONSTANT_MACROS -D__STDC_LIMIT_MACROS -O3 -g3 -I../include -lrt -pthread Voter.cpp ../libvoltcpp.a -lrt -o voter

../libvoltcpp.a:
	cd .. ; \
	./build_static.sh

clean:
	rm asynchelloworld helloworld
