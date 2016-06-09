@echo off

setlocal ENABLEEXTENSIONS

rem -- Data
pushd %~dp0
cd ..
set MAIN_DIR=%CD%
popd
set MAIN_SOLUTION=windows\voltdbclientcpp.sln
set MAIN_PROJECT=voltdbcpp
set COMMANDS=build clean rebuild
set CLEAN_COMMANDS=clean rebuild
set CONFIGURATIONS=release debug
set PLATFORMS=win32 x64

rem -- Initialize Visual Studio environment
set VS_LOCATION=
if defined VS110COMNTOOLS set VS_LOCATION=%VS110COMNTOOLS%\..\..\VC
if defined VS120COMNTOOLS set VS_LOCATION=%VS120COMNTOOLS%\..\..\VC
if defined VS130COMNTOOLS set VS_LOCATION=%VS130COMNTOOLS%\..\..\VC
if not defined VS_LOCATION (
    echo ** Visual Studio not found **
    goto exitfailure
)
call "%VS_LOCATION%\vcvarsall.bat"

rem -- Check the command
set COMMAND=
for %%C in (%COMMANDS%) do (
    if /I "%1" == "%%C" (
        set COMMAND=%%C
    )
)
if not defined COMMAND goto exitusage
shift

rem -- Check the configuration
set CONFIGURATION=
if not defined VOLTDB_VS_CONFIGURATION (
    set VOLTDB_VS_CONFIGURATION=%1
    shift
)
for %%C in (%CONFIGURATIONS%) do (
    if /I "%VOLTDB_VS_CONFIGURATION%" == "%%C" (
        set CONFIGURATION=%%C
    )
)
if not defined CONFIGURATION goto exitusage

rem -- Check the platform
set PLATFORM=
if not defined VOLTDB_VS_PLATFORM (
    set VOLTDB_VS_PLATFORM=%1
    shift
)
for %%P in (%PLATFORMS%) do (
    if /I "%VOLTDB_VS_PLATFORM%" == "%%P" (
        set PLATFORM=%%P
    )
)
if not defined PLATFORM goto exitusage

rem -- Clean?
set DO_CLEAN=no
for %%C in (%CLEAN_COMMANDS%) do (
    if /I "%COMMAND%" == "%%C" (
        set DO_CLEAN=yes
    )
)
if "%DO_CLEAN%" == "yes" (
    msbuild %MAIN_DIR%\%MAIN_SOLUTION% /t:clean /p:Configuration=%CONFIGURATION%;Platform=%PLATFORM%
    if errorlevel 1 (
        goto exitfailure
    )
)

rem -- Execute command body
goto %COMMAND%

rem -- Command body: clean (already took care of it above)
:clean
goto exitsuccess

rem -- Command body: build
:build
:rebuild
msbuild %MAIN_DIR%\%MAIN_SOLUTION% /t:%MAIN_PROJECT% /p:Configuration=%CONFIGURATION%;Platform=%PLATFORM%
if errorlevel 1 (
    goto exitfailure
)
goto exitsuccess

rem -- Exit: with usage message
:exitusage
echo Usage: %~n0 COMMAND CONFIGURATION PLATFORM
echo          COMMAND: %COMMANDS%
echo    CONFIGURATION: %CONFIGURATIONS%
echo         PLATFORM: %PLATFORMS%
endlocal
exit /b 1

rem -- Exit: with failure
:exitfailure
echo ** Command failed: %COMMAND% **
endlocal
exit /b 1

rem -- Exit: with success
:exitsuccess
echo :: Command succeeded: %COMMAND% ::
endlocal
