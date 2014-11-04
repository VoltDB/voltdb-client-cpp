# VoltDB C++ Client for Windows

For now the VoltDB Windows C++ client is only built as a static library.

## Building from the command line

Use your "favorite" method for creating a command prompt window with the
necessary environment for running Visual Studio command line tools. Any of
the following methods will work.

* Run the Developer Command Prompt from the Start Menu for Visual Studio.
* Run an ordinary command prompt and invoke VC\vcvarsall.bat from under the VS installation folder.
* Use some other ad hoc method for permanently setting the user environment.

### Running devenv directly

You can directly invoke devenv.exe with the appropriate options, e.g. to
build in 32 bit release mode:

```
devenv oadamip_voltdb.vcxproj /Project voltdbcpp /Build "win32|release"
```

Substitute "x64" for "win32" for a 64 bit build. Use "debug" instead of
"release" for a debug build.

The /Project option avoids building the test client executables.

Use /Clean instead of /Build to delete built objects in order to rebuild
everything.

### Using the run.bat script.

The tools\run.bat script constructs the appropriate command line and can
provide usage help. Run it with no arguments to see the usage message.

```
Usage: run build|clean|rebuild debug|release win32|x64
```
