@echo off

setlocal

set SOLUTION=..\windows\voltdbcpp\voltdbcpp.vcxproj
set PROJECT=voltdbcpp

:checkcommand
if /I "%1" == "build" goto checkconfig
if /I "%1" == "clean" goto checkconfig
if /I "%1" == "rebuild" goto checkconfig
goto usage

:checkconfig
if /I "%2" == "release" goto checkplatform
if /I "%2" == "debug" goto checkplatform
goto usage

:checkplatform
set CONFIGURATION=%1
if /I "%3" == "win32" goto clean
if /I "%3" == "x64" goto clean
goto usage

:clean
if /I "%1" == "build" goto build
devenv %~dp0%SOLUTION% /Project %PROJECT% /Clean "%2|%3"
if errorlevel 1 goto exitfailure
if /I "%1" == "clean" goto exitsuccess
goto build

:build
devenv %~dp0%SOLUTION% /Project %PROJECT% /Build "%2|%3"
if errorlevel 1 goto exitfailure
goto exitsuccess

:usage
echo Usage: %~n0 build^|clean^|rebuild debug^|release win32^|x64
goto exitfailure

:exitfailure
echo * Command failed *
endlocal
exit /b 1

:exitsuccess
endlocal
