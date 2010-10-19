@echo off 
set JAVA_HOME="C:\Program Files\Java\jdk1.5.0_07"
set HADOOP_HOME=D:\temp
set CLOUDATA_HOME=%~f0\..\..

set CLASSPATH=%CLOUDATA_HOME%;%CLOUDATA_HOME%\conf

for %%f in (%CLOUDATA_HOME%\cloudata*.jar) do call :oneStep %%f

for %%f in (%CLOUDATA_HOME%\lib\*.jar) do call :oneStep %%f

for %%f in (%CLOUDATA_HOME%\lib\jetty-ext\*.jar) do call :oneStep %%f

for %%f in (%HADOOP_HOME%\hadoop*.jar) do call :oneStep %%f


goto :theEnd1

:oneStep
REM echo "ARG: %1"
if "%CLASSPATH%" == "" (set CLASSPATH=%1) else (set CLASSPATH=%CLASSPATH%;%1)
exit /B

:theEnd1

for %%f in (%HADOOP_HOME%\hadoop*.jar) do call :oneStep %%f

rem echo %CLASSPATH%
call java -Xms256m -Xmx512m -Duser.dir=%CLOUDATA_HOME% -cp %CLASSPATH% %1 %2 %3 %4 %5 %6 %7