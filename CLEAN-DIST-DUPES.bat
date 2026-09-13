@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0."

echo(
echo ============================================================
echo  dfbnb-data  -  remove stale dist files tripping the
echo  "Verify internal links resolve (guide_index)" CI step
echo ============================================================
echo(
echo Working folder: %CD%
echo(

rem ---------- check we are in the repo ----------
if not exist ".git" (
  echo ERROR: no .git folder here, so this is not the repo root.
  echo Put this .bat directly in the dfbnb-data folder and run it again.
  echo(
  pause
  exit /b 1
)
echo   .git folder found - OK

rem ---------- find git.exe ----------
set "GIT="
for /f "delims=" %%G in ('where git 2^>nul') do if not defined GIT set "GIT=%%G"
if not defined GIT if exist "%ProgramFiles%\Git\cmd\git.exe" set "GIT=%ProgramFiles%\Git\cmd\git.exe"
if not defined GIT if exist "%ProgramFiles(x86)%\Git\cmd\git.exe" set "GIT=%ProgramFiles(x86)%\Git\cmd\git.exe"
if not defined GIT if exist "%LocalAppData%\Programs\Git\cmd\git.exe" set "GIT=%LocalAppData%\Programs\Git\cmd\git.exe"
if not defined GIT for /f "delims=" %%G in ('dir /b /s "%LocalAppData%\GitHubDesktop\git.exe" 2^>nul') do if not defined GIT set "GIT=%%G"

if not defined GIT (
  echo(
  echo ERROR: could not find git.exe on this machine.
  echo If you normally use GitHub Desktop only, tell Claude and it will
  echo write the steps out for GitHub Desktop instead.
  echo(
  pause
  exit /b 1
)
echo   git found: !GIT!
echo(

rem ---------- show what will go ----------
echo These files will be DELETED from the repo and from disk:
echo(
echo   dist/mini_seasons.json   ^(orphan - the live file is dist/mini_seasons/mini_seasons.json^)
echo(
"!GIT!" ls-files "*-WorkHorse.json" "*-WorkHorse.txt"
echo(
echo   ^(all of the above are OneDrive sync-conflict copies^)
echo(
echo Nothing else in your working tree gets committed - only these deletions
echo plus a new .gitignore line.
echo(
pause
echo(

echo --- removing orphan dist/mini_seasons.json ---
"!GIT!" rm --ignore-unmatch -- "dist/mini_seasons.json"

echo --- removing OneDrive conflict copies ---
"!GIT!" rm -r --ignore-unmatch -- "*-WorkHorse.json" "*-WorkHorse.txt"

echo --- adding ignore rule ---
findstr /c:"*-WorkHorse." .gitignore >nul 2>&1
if errorlevel 1 (
  echo.>>.gitignore
  echo # OneDrive sync-conflict copies ^(device name appended^) - never commit these>>.gitignore
  echo *-WorkHorse.json>>.gitignore
  echo *-WorkHorse.txt>>.gitignore
  echo   added to .gitignore
) else (
  echo   already in .gitignore, skipped
)
"!GIT!" add .gitignore

echo(
echo --- staged changes ---
"!GIT!" status --short --untracked-files=no
echo(
echo About to commit and push the above to origin/main.
pause

"!GIT!" commit -m "Remove stale dist artifacts breaking the guide-link check" -m "dist/mini_seasons.json is an orphan from the old flat layout - the builder writes dist/mini_seasons/mini_seasons.json - and the *-WorkHorse.json files are OneDrive sync-conflict copies. Both were frozen snapshots carrying the pre-rename junk farming URLs and failed the guide_index link check."
if errorlevel 1 (
  echo(
  echo Commit failed - nothing pushed. Read the message above.
  pause
  exit /b 1
)

"!GIT!" push origin main
if errorlevel 1 (
  echo(
  echo Push failed - the commit is still here locally. Read the message above.
  pause
  exit /b 1
)

echo(
echo ============================================================
echo  Done. The patch build will re-run on this push - watch the
echo  "Verify internal links resolve (guide_index)" step.
echo ============================================================
echo(
pause
