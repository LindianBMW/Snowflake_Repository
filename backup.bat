@echo off
setlocal

REM Set your source and backup root folder
set "SOURCE=C:\Users\lindian.thomas\Documents\Snowflake"
set "BACKUP_ROOT=C:\Users\lindian.thomas\Documents\Snowflake_Backups"

REM Get date in YYYYMMDD format
for /f "tokens=2 delims==." %%I in ('"wmic os get localdatetime /value"') do set datetime=%%I
set "DATE=%datetime:~0,8%"

REM Create backup folder with date
set "BACKUP_FOLDER=%BACKUP_ROOT%\Backup_%DATE%"
mkdir "%BACKUP_FOLDER%"

REM Copy all files and subfolders
xcopy "%SOURCE%\\*" "%BACKUP_FOLDER%\\" /E /I /H /Y

echo Backup complete: %BACKUP_FOLDER%
pause

