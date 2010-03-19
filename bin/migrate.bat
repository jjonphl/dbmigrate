@if "%DEBUG%" == "" @echo off

if "%OS%"=="Windows_NT" setlocal

set SCRIPTDIR=%~dp0
set DBMIGRATE_HOME=%SCRIPTDIR%..
set PROJECT_DIR=%cd%

groovy -cp %DBMIGRATE_HOME%\scripts -Dproj.dir=%PROJECT_DIR% %DBMIGRATE_HOME%\bin\migrate.groovy %1 %2 %3 %4 %5 %6 %7 %8 %9
