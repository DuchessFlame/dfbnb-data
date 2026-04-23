<#
  build-curv-points.ps1

  One-button regenerator for the CURV POINTS TSV. Run this AFTER xEdit has
  produced a fresh CURV_Export_<Month>_<Year>_CURV.tsv in dfbnb-data\tsv\.

  What it does:
    1. Finds the newest CURV_Export_*_CURV.tsv in dfbnb-data\tsv\.
    2. Points build-curv-points-tsv.ps1 (lives under fo76-tools) at that
       records TSV and the local curve-table JSON root.
    3. Writes the matching CURV_Export_<Month>_<Year>_POINTS.tsv next to it.

  Output filename convention matches what src\build_farming_guides_json.py
  and src\build_curves_json.py glob for (CURV_Export_*_POINTS.tsv).

  Everything is generative: re-running this from a fresh xEdit export
  produces a fully refreshed POINTS TSV with no hand-editing. Commit both
  files to dfbnb-data\main and CI rebuilds farming_guides.json and
  dist\curves\ from them.

  Usage (from anywhere):
      powershell -ExecutionPolicy Bypass -File path\to\dfbnb-data\tools\build-curv-points.ps1

  Or double-click this file in Explorer if .ps1 is associated with
  PowerShell on your machine.

  Optional override:
      -FO76ToolsRoot "D:\other\fo76-tools"
      -RecordsTsv    "C:\path\to\specific\CURV_Export_May_2026_CURV.tsv"
#>

[CmdletBinding()]
param(
  [string]$FO76ToolsRoot = "",
  [string]$RecordsTsv    = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ---- Paths ----------------------------------------------------------------
# This script should live in dfbnb-data\tools\. Resolve the repo root one
# level up so we find tsv\ regardless of where the script is invoked from.
$RepoRoot = Split-Path -Parent $PSScriptRoot
$TsvDir   = Join-Path $RepoRoot "tsv"

if (-not (Test-Path -LiteralPath $TsvDir)) {
  Write-Host "Expected tsv folder not found: $TsvDir" -ForegroundColor Red
  exit 1
}

# FO76 tools root: default to the sibling fo76-tools folder next to
# dfbnb-data, fall back to Duchess's canonical OneDrive path, then any
# override from the caller.
if ([string]::IsNullOrWhiteSpace($FO76ToolsRoot)) {
  $repoParent = Split-Path -Parent $RepoRoot
  $candidate  = Join-Path $repoParent "fo76-tools"
  if (Test-Path -LiteralPath $candidate) {
    $FO76ToolsRoot = $candidate
  } else {
    $FO76ToolsRoot = "C:\Users\Duche\OneDrive\GitHub\fo76-tools"
  }
}

if (-not (Test-Path -LiteralPath $FO76ToolsRoot)) {
  Write-Host "fo76-tools not found at: $FO76ToolsRoot" -ForegroundColor Red
  Write-Host "Pass -FO76ToolsRoot <path> or clone fo76-tools next to dfbnb-data." -ForegroundColor Yellow
  exit 1
}

$JsonRoot = Join-Path $FO76ToolsRoot "misc\curvetables\json"
if (-not (Test-Path -LiteralPath $JsonRoot)) {
  Write-Host "Curve-table JSON root missing: $JsonRoot" -ForegroundColor Red
  exit 1
}

$Converter = Join-Path $FO76ToolsRoot "misc\curvetables\build-curv-points-tsv.ps1"
if (-not (Test-Path -LiteralPath $Converter)) {
  Write-Host "Converter script missing: $Converter" -ForegroundColor Red
  exit 1
}

# ---- Find the records TSV to convert -------------------------------------
if ([string]::IsNullOrWhiteSpace($RecordsTsv)) {
  # Prefer files that match the canonical "*_CURV.tsv" suffix. If none
  # exist yet (e.g. an older "*.tsv_CURV.tsv" export is sitting there),
  # fall back to any CURV_Export_* that isn't a POINTS file.
  $primary = Get-ChildItem -LiteralPath $TsvDir -File `
              -Filter "CURV_Export_*_CURV.tsv" -ErrorAction SilentlyContinue |
             Sort-Object LastWriteTime -Descending

  if ($primary) {
    $RecordsTsv = $primary[0].FullName
  } else {
    $fallback = Get-ChildItem -LiteralPath $TsvDir -File `
                  -Filter "CURV_Export_*.tsv" -ErrorAction SilentlyContinue |
                Where-Object { $_.Name -notmatch "_POINTS\.tsv$" -and `
                               $_.Name -notmatch "CurvePoints\.tsv$" } |
                Sort-Object LastWriteTime -Descending
    if ($fallback) {
      $RecordsTsv = $fallback[0].FullName
    }
  }
}

if ([string]::IsNullOrWhiteSpace($RecordsTsv) -or `
    -not (Test-Path -LiteralPath $RecordsTsv)) {
  Write-Host "No CURV records TSV found in $TsvDir" -ForegroundColor Red
  Write-Host "Run the xEdit ExportCURVToTSV.pas script first." -ForegroundColor Yellow
  exit 1
}

# ---- Derive output filename ----------------------------------------------
# Input like "CURV_Export_Apr_2026_CURV.tsv" -> "CURV_Export_Apr_2026_POINTS.tsv"
# Input like "CURV_Export_Apr_2026.tsv"      -> "CURV_Export_Apr_2026_POINTS.tsv"
$recordsName = [System.IO.Path]::GetFileName($RecordsTsv)
if ($recordsName -match '^(?<stem>CURV_Export_[A-Za-z]+_\d{4})') {
  $stem = $Matches['stem']
} else {
  # Last-ditch: drop the extension and strip a trailing _CURV if present.
  $stem = [System.IO.Path]::GetFileNameWithoutExtension($recordsName)
  $stem = $stem -replace '_CURV$', ''
}

$OutTsv = Join-Path $TsvDir ("$stem" + "_POINTS.tsv")

# ---- Run converter --------------------------------------------------------
Write-Host ""
Write-Host "=== CURV points builder ===" -ForegroundColor Cyan
Write-Host "  Records TSV : $RecordsTsv"
Write-Host "  JSON root   : $JsonRoot"
Write-Host "  Output TSV  : $OutTsv"
Write-Host ""

& $Converter -CurvTsv $RecordsTsv -JsonRoot $JsonRoot -OutTsv $OutTsv

if ($LASTEXITCODE -ne 0 -and $null -ne $LASTEXITCODE) {
  Write-Host "Converter failed (exit $LASTEXITCODE)" -ForegroundColor Red
  exit $LASTEXITCODE
}

# ---- Sanity check ---------------------------------------------------------
if (-not (Test-Path -LiteralPath $OutTsv)) {
  Write-Host "Converter finished but no output file appeared at: $OutTsv" -ForegroundColor Red
  exit 1
}

$lineCount = (Get-Content -LiteralPath $OutTsv | Measure-Object -Line).Lines
Write-Host ""
Write-Host "=== Done ===" -ForegroundColor Green
Write-Host "  $OutTsv"
Write-Host "  $lineCount lines (including header)"
Write-Host ""
Write-Host "Next: commit the new *_CURV.tsv and *_POINTS.tsv to dfbnb-data\main."
Write-Host "      CI will rebuild dist\farming_guides.json and dist\curves\."
