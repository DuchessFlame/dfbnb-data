@echo off
REM ===========================================================================
REM  run_all_maps.bat - render every farming spawn map in one go.
REM
REM  Double-click it, or run it from a terminal in dfbnb-data. Same thing as:
REM      python src\render_all_maps.py
REM  but it sets the two paths first so you don't have to.
REM
REM  Change these if Mappalachia or "Guides and Stuff" ever move.
REM ===========================================================================

set "MAPPALACHIA_DIR=D:\Mappalachia"
set "GUIDES_ROOT=C:\Users\Duche\OneDrive\Guides and Stuff"

cd /d "%~dp0"

echo.
echo Rendering every farming spawn map. This takes a while - leave it running.
echo.

python src\render_all_maps.py %*

echo.
if errorlevel 1 (
  echo Finished WITH ERRORS - scroll up for the "!!" lines.
) else (
  echo Finished.
)
echo.
pause
