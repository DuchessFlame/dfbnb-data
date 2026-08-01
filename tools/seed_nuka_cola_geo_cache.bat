@echo off
REM ============================================================================
REM  Seed / refresh the Nuka Cola spawn geo cache from the LOCAL Mappalachia DB.
REM
REM  Run this ONCE (and again whenever the game data / TSV exports change). It is
REM  the only step that needs the Mappalachia DB. It writes:
REM     data\nuka_cola_spawns\geo_cache.json   (coords + region per placement)
REM     dist\nuka_cola_spawns_*.json           (the 12 page JSONs + manifest)
REM  After this, the weekly scheduled task and the GitHub workflows rebuild the
REM  pages automatically with NO database needed.
REM
REM  Just double-click this file. Needs Python 3 — no pip installs required.
REM  If your Mappalachia DB lives elsewhere, edit the MAPPALACHIA_DB line below.
REM ============================================================================
setlocal
set "MAPPALACHIA_DB=D:\Mappalachia\data\mappalachia.db"
cd /d "%~dp0.."

if not exist "%MAPPALACHIA_DB%" (
  echo [ERROR] Mappalachia DB not found at:
  echo         %MAPPALACHIA_DB%
  echo Edit this .bat and fix the MAPPALACHIA_DB path, then run again.
  pause
  exit /b 1
)

echo Building Nuka Cola spawns from %MAPPALACHIA_DB% ...
echo.
where python >nul 2>nul
if %errorlevel%==0 (
  python src\build_nuka_cola_spawns_json.py
) else (
  py src\build_nuka_cola_spawns_json.py
)

echo.
echo ============================================================================
echo Done. Commit/push these to GitHub so the live site updates:
echo    dist\nuka_cola_spawns_*.json
echo    dist\nuka_cola_spawns_manifest.json
echo    data\nuka_cola_spawns\geo_cache.json
echo ============================================================================
pause
