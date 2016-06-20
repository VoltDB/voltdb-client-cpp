# VoltDB C++ Client for Windows

For now the VoltDB Windows C++ client is only built as a static library.

Prerequisites and environment setup
=========================================

- Visual Studio 2013 or better.

- Boost C++ Library 1.54 or better. Windows build was tested with Boost
C++ library 1.56 -http://sourceforge.net/projects/boost/files/boost/1.56.0/boost_1_56_0.tar.gz/download,
32 and 64 bit version of it. Following are steps that can be used to build Boost 32 and 64 bit locally

32 Bit version:
Switch to the directory location where boost libraries have been extracted.
Run following steps to build and install 32 bit version of Boost libraries.
1 - bootstrap.bat
2 - b2.exe --prefix=c:\Boost --libdir=c:\Boost\lib\i386 install

64 Bit version:
Switch to the directory location where boost libraries have been extracted.
Run following steps to build and install 64 bit version of Boost libraries.
1 - bootstrap.bat
2 - b2.exe --prefix=c:\Boost --libdir=c:\Boost\lib\x64 architecture=x86 address-model=64 install

Create the necessary BOOST environment variables for the VS project:
- "BOOST_INCLUDE" set to path where the include boost directory that contains all the header files.
For above installation, set the BOOST_INCLUDE to "C:\Boost\include\boost-1_56". If properly set,
"echo %BOOST_INCLUDE%" will output "C:\Boost\include\boost-1_56".

- "BOOST_LIB_32" set to path where the BOOST 32 libraries are located. For above installation,
set the BOOST_LIB_32 to "C:\Boost\lib\i386". If properly set, "echo %BOOST_LIB_32%" will output
"C:\Boost\lib\i386"

- "BOOST_LIB_64" set to path where the BOOST 64 libraries are located. For above installation,
the BOOST_LIB_64 will be "C:\Boost\lib\x64". If properly set, "echo %BOOST_LIB_64%" will output
for example "C:\Boost\lib\x64".


## Building from the command line

Use your "favorite" method for creating a command prompt window with the
necessary environment for running Visual Studio command line tools. Any of
the following methods will work.

* Run the Developer Command Prompt from the Start Menu for Visual Studio.
* Run an ordinary command prompt and invoke VC\vcvarsall.bat from under the VS installation folder.
* Use some other ad hoc method for permanently setting the user environment.

### Using the run.bat script.

The tools\run.bat script constructs the appropriate command line and can
provide usage help. Run it with no arguments to see the usage message.

```
Usage: run build|clean|rebuild debug|release win32|x64
```
