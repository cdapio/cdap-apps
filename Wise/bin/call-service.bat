@echo OFF

REM #################################################################################
REM ##
REM ## Copyright © 2014 Cask Data, Inc.
REM ##
REM ## Licensed under the Apache License, Version 2.0 (the "License"); you may not
REM ## use this file except in compliance with the License. You may obtain a copy of
REM ## the License at
REM ##
REM ## http://www.apache.org/licenses/LICENSE-2.0
REM ##
REM ## Unless required by applicable law or agreed to in writing, software
REM ## distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
REM ## WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
REM ## License for the specific language governing permissions and limitations under
REM ## the License.
REM ##
REM #################################################################################

REM Tool to call the endpoints defined by the WiseService

SET APP_NAME=Wise
SET SERVICE_NAME=WiseService

REM Set the base directory
for %%i in ("%~dp0..\") do (SET APP_HOME=%%~dpi)

REM Set path for curl.exe
SET PATH=%APP_HOME%\..\..\libexec\bin

REM Process access token
SET ACCESS_TOKEN=
SET ACCESS_TOKEN_FILE=%HOMEPATH%\.cdap.accesstoken
if exist %ACCESS_TOKEN_FILE% set /p ACCESS_TOKEN=<%ACCESS_TOKEN_FILE%

REM Process Command line
IF %1.==. GOTO USAGE
IF %2.==. CALL :COUNT_IP %1
CALL :COUNT_IP_URL %1 %2

:USAGE
echo Tool to call the endpoints defined by the WiseService
echo Usage: %0 <ip_address> <uri>
GOTO :EOF

:COUNT_IP
SET IP=%~1
CALL :GET %APP_NAME% %SERVICE_NAME% "ip/%IP%/count"
GOTO :EOF

:COUNT_IP_URL
SET IP=%~1
SET URI=%~2
CALL :GET %APP_NAME% %SERVICE_NAME% "ip/%IP%/uri/%URI%/count"
GOTO :EOF

:GET
SET ACTION=%~1

echo %ACTION% %PROGRAM_NAME% for application %APP%
curl -H "Authorization: Bearer %ACCESS_TOKEN%" -X GET -sL http://localhost:10000/v2/apps/%APP%/services/%SERVICE_NAME%/methods/%ACTION%
echo.
GOTO :EOF
