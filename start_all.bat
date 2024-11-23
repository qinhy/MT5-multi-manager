@echo off
setlocal enabledelayedexpansion

REM Path to the .env file
set FILE=.env

REM Loop through the .env file and set variables
for /f "usebackq tokens=* delims=" %%a in ("%FILE%") do (
    set "line=%%a"
    REM Skip empty lines and lines starting with #
    if not "!line!"=="" if "!line:~0,1!" neq "#" (
        for /f "tokens=1,2 delims==" %%b in ("!line!") do (
            set %%b=%%c
        )
    )
)
set APP_INVITE_CODE
set APP_SECRET_KEY

@echo off
start .\bin\api.bat
start .\bin\flower.bat
start .\bin\gui.bat
start .\bin\redis.bat
start .\bin\worker.bat




