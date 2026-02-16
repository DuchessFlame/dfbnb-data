# build_titles_images_manifest_from_tsv.ps1
# Build titles_images_manifest.json in the EXACT format extract_titles_storefront_images_local.py expects:
# { "tasks": [ { "entitlementEdids": [...], "ddsPaths": [...] } ] }
#
# Uses ETIP + ETDI only (ignores condition fields entirely).

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$TsvPath = "C:\Users\Duche\OneDrive\GitHub\dfbnb-data\ENTM_Export_March_2026.tsv"
$OutPath = "C:\Users\Duche\OneDrive\GitHub\dfbnb-data\dist\titles_images_manifest.json"

if (!(Test-Path -LiteralPath $TsvPath)) { throw "TSV not found: $TsvPath" }

$lines = Get-Content -LiteralPath $TsvPath -Encoding Default
if ($lines.Count -lt 2) { throw "TSV looks empty: $TsvPath" }

$header = $lines[0].Split("`t")

function ColIndex($name) {
  $idx = [Array]::IndexOf($header, $name)
  if ($idx -lt 0) { throw "Missing required column: $name" }
  return $idx
}

$iEDID = ColIndex "EDID"
$iETIP = ColIndex "ETIP"
$iETDI = ColIndex "ETDI"

function NormRel([string]$p) {
  if ([string]::IsNullOrWhiteSpace($p)) { return "" }
  $p = $p.Trim().Replace("\", "/")
  $p = $p -replace "^/+", ""
  return $p
}

# Build one task per title: entitlementEdids=[edid], ddsPaths=["textures/.../file.dds"]
$tasks = New-Object System.Collections.Generic.List[object]

for ($i = 1; $i -lt $lines.Count; $i++) {
  $line = $lines[$i]
  if ([string]::IsNullOrWhiteSpace($line)) { continue }

  $parts = $line.Split("`t")
  if ($parts.Count -le $iETDI) { continue }

  $edid = ($parts[$iEDID]).Trim()
  $etip = ($parts[$iETIP]).Trim()
  $etdi = ($parts[$iETDI]).Trim()

  if ([string]::IsNullOrWhiteSpace($edid)) { continue }
  if ([string]::IsNullOrWhiteSpace($etip)) { continue }
  if ([string]::IsNullOrWhiteSpace($etdi)) { continue }

  $ent = $edid.ToLowerInvariant()

  $path = (NormRel $etip) + (NormRel $etdi)

  # Ensure it is "textures/..." style (converter normalizes this)
  $pathLower = $path.ToLowerInvariant()
  if ($pathLower.StartsWith("textures/")) {
    $path = $path  # keep
  } else {
    # if TSV already stripped "Textures/", add it back
    $path = "Textures/" + $path
  }

  # Normalize to forward slashes and lowercase-ish matching behavior
  $path = $path.Replace("\", "/")

  $tasks.Add([PSCustomObject]@{
    entitlementEdids = @($ent)
    ddsPaths         = @($path)
  })
}

$manifestObj = [PSCustomObject]@{ tasks = $tasks }

$outDir = Split-Path -Parent $OutPath
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

$manifestObj | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $OutPath -Encoding UTF8

Write-Host "OK: wrote $($tasks.Count) tasks to $OutPath"