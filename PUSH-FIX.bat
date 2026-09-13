@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0."

echo(
echo ============================================================
echo  dfbnb-data  -  finish the push
echo  (the cleanup commit is already made; GitHub just has newer
echo   commits from the CI runs that you do not have locally)
echo ============================================================
echo(
echo Working folder: %CD%
echo(

if not exist ".git" (
  echo ERROR: no .git folder here. Put this .bat in the dfbnb-data folder.
  pause
  exit /b 1
)

set "GIT="
for /f "delims=" %%G in ('where git 2^>nul') do if not defined GIT set "GIT=%%G"
if not defined GIT if exist "%ProgramFiles%\Git\cmd\git.exe" set "GIT=%ProgramFiles%\Git\cmd\git.exe"
if not defined GIT if exist "%ProgramFiles(x86)%\Git\cmd\git.exe" set "GIT=%ProgramFiles(x86)%\Git\cmd\git.exe"
if not defined GIT if exist "%LocalAppData%\Programs\Git\cmd\git.exe" set "GIT=%LocalAppData%\Programs\Git\cmd\git.exe"
if not defined GIT for /f "delims=" %%G in ('dir /b /s "%LocalAppData%\GitHubDesktop\git.exe" 2^>nul') do if not defined GIT set "GIT=%%G"
if not defined GIT (
  echo ERROR: could not find git.exe.
  pause
  exit /b 1
)
echo   git found: !GIT!
echo(

echo --- fetching what GitHub has ---
"!GIT!" fetch origin
echo(
echo --- commits on GitHub that you do not have locally ---
"!GIT!" log --oneline HEAD..origin/main
echo(
echo --- your commit(s) not yet on GitHub ---
"!GIT!" log --oneline origin/main..HEAD
echo(
echo Next step: replay your cleanup commit on top of those GitHub commits.
echo Any uncommitted work in your folder is stashed and put back automatically.
echo(
pause
echo(

echo --- rebasing onto origin/main ---
"!GIT!" -c rebase.autoStash=true rebase origin/main
if errorlevel 1 (
  echo(
  echo Rebase hit a problem - backing out so your repo is left as it was.
  "!GIT!" rebase --abort
  echo(
  echo Nothing was pushed and nothing was lost. Screenshot the message
  echo above and send it to Claude.
  echo(
  pause
  exit /b 1
)

echo(
echo --- pushing ---
"!GIT!" push origin main
if errorlevel 1 (
  echo(
  echo Push still failed - read the message above. The commit is safe locally.
  echo(
  pause
  exit /b 1
)

echo(
echo ============================================================
echo  Pushed. The patch build re-runs on this push - watch the
echo  "Verify internal links resolve (guide_index)" step.
echo ============================================================
echo(
"!GIT!" status --short --untracked-files=no
echo(
pause
