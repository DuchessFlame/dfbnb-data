@echo off
setlocal
cd /d "%~dp0."

echo(
echo ============================================================
echo  dfbnb-data  -  refresh the server picture list
echo  Run this after every FileZilla upload.
echo  Pull in GitHub Desktop FIRST.
echo ============================================================
echo(
echo  Type the SFTP password (same as FileZilla) when asked.
echo  Nothing shows while you type - that is normal. Press Enter.
echo(

python tools\list_server_images.py --host buffsnbrew1.sftp.wpengine.com --port 2222 --user buffsnbrew1-nav || goto :fail

echo(
echo ============================================================
echo  Done. IGNORE the "Next: build_image_index.py" line above.
echo  Now in GitHub Desktop:
echo    1. Tick data\server_listing.tsv
echo    2. Summary: server listing
echo    3. Commit to main, then Push origin
echo  Then GitHub - Actions - Refresh Images: wait for the green
echo  tick (about 4 minutes) and Ctrl + F5 the page.
echo ============================================================
echo(
pause
exit /b 0

:fail
echo(
echo FAILED - see the message above.
echo  "Authentication failed" = wrong password, run it again.
echo  "paramiko is not installed" = run: python -m pip install paramiko
pause
exit /b 1
