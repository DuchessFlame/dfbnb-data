@echo off
setlocal
cd /d "%~dp0."

echo(
echo ============================================================
echo  dfbnb-data  -  refresh the shared image library
echo  Run tools\list_server_images.py first (after any upload).
echo  No game-file rebuild: this only re-points images.
echo ============================================================
echo(

if not exist "data\server_listing.tsv" (
  echo ERROR: data\server_listing.tsv is missing.
  echo Run:  python tools\list_server_images.py --filezilla "YOUR SITE NAME"
  pause
  exit /b 1
)

echo [1/5] Building dist\image_index.json from the server listing ...
python src\build_image_index.py || goto :fail

echo [2/5] Plan checklists (plan_master, New Plans) ...
python src\add_plan_images.py dist\plan_master.json dist\new_plans.json || goto :fail
python src\add_plan_images.py --folder underarmour dist\underarmour.json || goto :fail
python src\add_plan_images.py --folder apparel-without-plans dist\no_plan_apparel.json || goto :fail

echo [3/5] Re-building the index with the plan checklists' new picks ...
python src\build_image_index.py || goto :fail

echo [4/5] Seasonal, mutated, Daily Ops, activities, public events, treasure maps ...
python src\apply_image_index.py || goto :fail

echo [5/5] Done.
echo(
echo  To-do list : audits\missing_images.md
echo  Tidy list  : audits\image_cleanup.md
echo  Commit     : data\server_listing.tsv, dist\image_index.json, dist\missing_images.json,
echo               audits\*.md and every changed dist\ file (git status shows them).
echo(
pause
exit /b 0

:fail
echo(
echo FAILED - see the message above. Nothing after that step ran.
pause
exit /b 1
