@echo off
setlocal

REM Set your source and backup root folder
set "SOURCE=C:\Users\lindian.thomas\Documents\Snowflake"
set "BACKUP_ROOT=C:\Users\lindian.thomas\Documents\Snowflake_Backups"



REM Get date and time in YYYY-MM-DD_HH-MM-SS format
for /f "tokens=2 delims==." %%I in ('"wmic os get localdatetime /value"') do set datetime=%%I
set "YYYY=%datetime:~0,4%"
set "MM=%datetime:~4,2%"
set "DD=%datetime:~6,2%"
set "HH=%datetime:~8,2%"
set "MI=%datetime:~10,2%"
set "SS=%datetime:~12,2%"
set "STAMP=%YYYY%-%MM%-%DD%_%HH%-%MI%-%SS%"

REM Create backup folder with formatted timestamp
set "BACKUP_FOLDER=%BACKUP_ROOT%\Backup_%STAMP%"
mkdir "%BACKUP_FOLDER%"

REM Copy all files and subfolders
xcopy "%SOURCE%\\*" "%BACKUP_FOLDER%\\" /E /I /H /Y

echo Backup complete: %BACKUP_FOLDER%
pause

