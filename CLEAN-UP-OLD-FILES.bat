@echo off
cd /d "%~dp0"
echo.
echo Moving old backup and duplicate files into a _to_delete folder...
echo Nothing is deleted - you can look at them first.
echo.
powershell -ExecutionPolicy Bypass -File "%~dp0move-dead-files.ps1"
echo.
pause
