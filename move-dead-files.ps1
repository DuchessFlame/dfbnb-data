<#
  move-dead-files.ps1
  Moves superseded backups, OneDrive conflict copies and scratch files into
  a dated _to_delete folder. Nothing is deleted - review the folder, then
  delete it yourself once you are happy.

  Run:   powershell -ExecutionPolicy Bypass -File .\move-dead-files.ps1
  Check: add  -WhatIf   to see what would move without moving anything.
#>
[CmdletBinding(SupportsShouldProcess = $true)]
param()

$Repo = "C:\Users\Duche\OneDrive\GitHub\dfbnb-data"
$Site = "C:\Users\Duche\OneDrive\Guides and Stuff\Json Files for Website\1 site-data"
$Bin  = Join-Path $Repo ("_to_delete\" + (Get-Date -Format "yyyy-MM-dd"))

$targets = @(
  # ---- superseded dated backups: src ----
  "$Repo\src\build_allies_pets_weather_json.py.bak-20260827-155231"
  "$Repo\src\build_bnb_menu_sync.py.bak-20260824-234529"
  "$Repo\src\build_camp_items_json.py.bak-20260826-094511"
  "$Repo\src\build_camp_items_json.py.bak-20260826-190246"
  "$Repo\src\build_camp_items_json.py.bak-20260827-160327"
  "$Repo\src\spawns_configs\plants.py.bak"
  "$Repo\src\spawns_configs\plants.py.harvestfix-20260824-164221.bak"

  # ---- superseded dated backups: workflows ----
  "$Repo\.github\workflows\build_camp_items.yml.bak-20260827-155414"
  "$Repo\.github\workflows\build-allies-pets-weather.yml.bak-20260827-155414"
  "$Repo\.github\workflows\build-buff-stations.yml.bak-20260827-155414"
  "$Repo\.github\workflows\dfbnb-patch-build.yml.bak-20260827-155414"
  "$Repo\.github\workflows\dfbnb-patch-build.yml.bak-20260902"
  "$Repo\.github\workflows\dfbnb-patch-build.yml.bak-prearmour-20260903"
  "$Repo\.github\workflows\dfbnb-patch-build.yml.bak2-20260902"
  "$Repo\.github\workflows\dfbnb-patch-build.yml.fixbak-20260824-142821"
  "$Repo\.github\workflows\dfbnb-pts-build.yml.bak-20260827-155414"
  "$Repo\.github\workflows\dfbnb-pts-build.yml.bak-20260902"
  "$Repo\.github\workflows\dfbnb-pts-build.yml.bak-prearmour-20260903"
  "$Repo\.github\workflows\dfbnb-pts-build.yml.bak2-20260902"
  "$Repo\.github\workflows\dfbnb-pts-build.yml.fixbak-20260824-142821"

  # ---- OneDrive conflict copies ("WorkHorse" = this PC's name) ----
  "$Repo\src\plan-system\plan_master-WorkHorse.json"
  "$Repo\tools\build-seasonal-fish-json-WorkHorse.mjs"
  "$Site\json\dfbnb-child\assets\df-bnb-spawns-WorkHorse.js"
  "$Site\json\dfbnb-child\assets\df-bnb-farming-non-perishable-guide-WorkHorse.js"

  # ---- scratch / test leftovers ----
  "$Repo\src\_head_test_upcoming.py"
  "$Repo\src\_synctest.py"
  "$Repo\src\_sync_test.txt"
  "$Repo\src\_test_write.txt"
  "$Repo\src\_urc_committed_test.py"
  "$Repo\src\.__head_test.py"
  "$Repo\tmp_render_chems.js"
  "$Repo\tmp_render_drink.js"
  "$Repo\tmp_render_farm.js"
  "$Repo\tmp_render_meat.js"
)

$moved = 0; $missing = 0; $bytes = 0

foreach ($t in $targets) {
  if (-not (Test-Path -LiteralPath $t)) {
    Write-Host "  skip (not found): $t" -ForegroundColor DarkGray
    $missing++
    continue
  }

  # keep the folder structure inside the bin so nothing collides
  if ($t.StartsWith($Repo))      { $rel = "dfbnb-data\" + $t.Substring($Repo.Length).TrimStart('\') }
  elseif ($t.StartsWith($Site))  { $rel = "site-data\"  + $t.Substring($Site.Length).TrimStart('\') }
  else                           { $rel = Split-Path $t -Leaf }

  $dest    = Join-Path $Bin $rel
  $destDir = Split-Path $dest -Parent
  if (-not (Test-Path -LiteralPath $destDir)) {
    New-Item -ItemType Directory -Path $destDir -Force | Out-Null
  }

  $size = (Get-Item -LiteralPath $t).Length
  if ($PSCmdlet.ShouldProcess($t, "move to $dest")) {
    Move-Item -LiteralPath $t -Destination $dest -Force
    Write-Host "  moved: $rel" -ForegroundColor Green
    $moved++; $bytes += $size
  }
}

Write-Host ""
Write-Host ("Moved {0} file(s), {1:N1} MB, into:" -f $moved, ($bytes / 1MB))
Write-Host "  $Bin"
if ($missing) { Write-Host "$missing already gone." -ForegroundColor DarkGray }
Write-Host ""
Write-Host "Review that folder, then delete it. Nothing else was touched."
