@echo off
where chcp >nul 2>nul
set flag=%ERRORLEVEL%
if %flag% == 0 (
    chcp 936
    mode 120,35
    for /F "delims=" %%X in ('where powershell') DO set PSHELL=%%X
    if not "%PSHELL%" == "" (
        powershell -command "&{$H=get-host;$W=$H.ui.rawui;$B=$W.buffersize;$B.width=120;$B.height=999;$W.buffersize=$B;}"
    )
)
title DIS Agent

cd /d "%~dp0"
cd ..

set MAIN_CLASS=com.huaweicloud.dis.agent.Agent
    
set JAVACMD=java
set JAVA_START_HEAP=256m
set JAVA_MAX_HEAP=512m

java -version >nul 2>nul
if not %ERRORLEVEL% == 0 (
    echo No java found, please install JRE1.8+.
    pause
    exit 1
)

set LIB_DIR=.\lib
set CONFIG_DIR=.\conf
set JVM_ARGS=-Xms%JAVA_START_HEAP% -Xmx%JAVA_MAX_HEAP% -Djava.io.tmpdir=%LIB_DIR% %JVM_ARGS% -Dlog4j.configurationFile=conf\log4j2.xml

rem clean sqlite dll
if exist "%LIB_DIR%\sqlite*-sqlitejdbc.dll" (
    del /F /Q %LIB_DIR%\sqlite*-sqlitejdbc.dll
    if exist "%LIB_DIR%\sqlite*-sqlitejdbc.dll" (
        echo DIS Agent is running, please stop it first.
        pause
        exit 1
    )
)

if "%MAIN_CLASS_ARGS%" == "" (
    set MAIN_CLASS_ARGS=-c .\conf\agent.yml
)

%JAVACMD% %JVM_ARGS% %JVM_DBG_OPTS% -Dfile.encoding="UTF-8" -cp "%LIB_DIR%\*;%CONFIG_DIR%" %MAIN_CLASS% %MAIN_CLASS_ARGS%
pause
