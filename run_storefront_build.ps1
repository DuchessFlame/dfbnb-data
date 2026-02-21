# run_storefront_build.ps1
# Deterministic: ONE manifest, convert only DDS that exist, output to ONE folder, lowercase filenames.

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ===== Paths (edit only if you move folders) =====
$manifest = "C:\Users\Duche\OneDrive\GitHub\dfbnb-data\dist\titles_images_manifest.json"

# IMPORTANT:
# This should be the folder that CONTAINS "textures\..."
# Example: <root>\textures\<...dds>
$extractedRoot = "C:\Users\Duche\OneDrive\GitHub\fo76-tools"

$toolsDir = "C:\Users\Duche\OneDrive\GitHub\fo76-tools"

# Export dir (python will create/use "storefront" under here)
$exportDir = "C:\Users\Duche\OneDrive\Guides and Stuff\Json Files for Website\1 site-data\json\uploads"
$outStorefront = Join-Path $exportDir "storefront"

$pyScript = Join-Path $PSScriptRoot "src\extract_titles_storefront_images_local.py"

function Assert-Path($p, $label) {
  if (-not (Test-Path -LiteralPath $p)) { throw "$label not found: $p" }
}

Assert-Path $manifest "Manifest"
Assert-Path $extractedRoot "Extracted root"
Assert-Path $toolsDir "Tools dir"
Assert-Path $pyScript "Python script"

New-Item -ItemType Directory -Force -Path $outStorefront | Out-Null

Write-Host "=== Storefront build ==="
Write-Host "Manifest:        $manifest"
Write-Host "Extracted root:  $extractedRoot"
Write-Host "Tools dir:       $toolsDir"
Write-Host "Export dir:      $exportDir"
Write-Host "Output folder:   $outStorefront"
Write-Host ""

# Rebuild manifest from TSV (ETIP+ETDI only) every run, so it's deterministic.
$manifestBuilder = Join-Path $repoRoot "build_titles_images_manifest_from_tsv.py"
Assert-Path $manifestBuilder "Manifest builder"
powershell -ExecutionPolicy Bypass -File $manifestBuilder

# Quick sanity: confirm you actually have DDS files under extracted root
$ddsCount = (Get-ChildItem -LiteralPath $extractedRoot -Recurse -File -Filter "*.dds" -ErrorAction SilentlyContinue | Measure-Object).Count
Write-Host "DDS files found under extracted root: $ddsCount"
if ($ddsCount -eq 0) {
  throw "No .dds files found under: $extractedRoot  (your extracted root is wrong, or DDS not extracted)"
}

python $pyScript `
  --manifest "$manifest" `
  --extracted-textures "$extractedRoot" `
  --tools-dir "$toolsDir" `
  --export-dir "$exportDir"

# Force lowercase filenames for Linux servers
$renamed = 0
Get-ChildItem -LiteralPath $outStorefront -File -Filter "*.webp" | ForEach-Object {
  $lower = $_.Name.ToLowerInvariant()
  if ($_.Name -ne $lower) {
    Move-Item -LiteralPath $_.FullName -Destination (Join-Path $outStorefront $lower) -Force
    $renamed++
  }
}

$built = (Get-ChildItem -LiteralPath $outStorefront -File -Filter "*.webp" | Measure-Object).Count
Write-Host ""
Write-Host "Lowercase rename count: $renamed"
Write-Host "Built WebPs: $built"
Write-Host "=== Done ==="