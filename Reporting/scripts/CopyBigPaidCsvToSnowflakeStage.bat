@echo off
REM === CONFIGURABLE VARIABLES ===
set "ROBOCOPY_EXE=C:\Windows\System32\robocopy.exe"
set "SRC_DIR=C:\Users\lindian.thomas\Big Motoring World Group\BIG Business Intelligence - FilesToConvertToCSV"
set "DEST_DIR=C:\Temp"
set "LOG_FILE=C:\Temp\sp_sync.log"
set "SNOWSQL=snowsql"
set "SNOW_CONN=bigmotoringworld_snowflake"
set "SNOW_STAGE=@reporting.details.REPORTS"

REM === STEP 1: Sync files from SharePoint-synced folder to destination ===
%ROBOCOPY_EXE% "%SRC_DIR%" "%DEST_DIR%" /E /Z /FFT /XO /R:2 /W:5 /XJ /NP /LOG+:"%LOG_FILE%"

REM === STEP 2: Remove all files from the Snowflake stage ===
%SNOWSQL% -c %SNOW_CONN% -q "REMOVE %SNOW_STAGE%"

REM === STEP 3: Upload the first CSV file to the stage ===
set "CSV_FILE=big paid 2025 ytd.csv"

if exist "%DEST_DIR%\%CSV_FILE%" (
    echo Uploading %CSV_FILE%...
    set "FILE_URI=file://%DEST_DIR:/=/%/%CSV_FILE: = %/""
    set "FILE_URI=%FILE_URI:\=/%""
    %SNOWSQL% -c %SNOW_CONN% -q "PUT 'file://C:/Temp/%CSV_FILE%' %SNOW_STAGE% AUTO_COMPRESS=false"
) else (
    echo WARNING: File not found: %DEST_DIR%\%CSV_FILE%
)

REM === STEP 4: Upload the second CSV file to the stage ===
set "CSV_FILE2=bigmw1_daily report.csv"

if exist "%DEST_DIR%\%CSV_FILE2%" (
    echo Uploading %CSV_FILE2%...
    %SNOWSQL% -c %SNOW_CONN% -q "PUT 'file://C:/Temp/%CSV_FILE2%' %SNOW_STAGE% AUTO_COMPRESS=false"
) else (
    echo WARNING: File not found: %DEST_DIR%\%CSV_FILE2%
)