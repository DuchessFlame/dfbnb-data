# build_titles_images_manifest_from_tsv.ps1
# Builds dist\titles_images_manifest.json from an xEdit TSV export using ETIP + ETDI only.
# Output format: [{ entitlement: "...", dds: "ATX\...\file.dds" }, ...]

$ErrorActionPreference = "Stop"

$TsvPath = "C:\Users\Duche\OneDrive\GitHub\dfbnb-data\ENTM_Export_March_2026.tsv"
$OutPath = "C:\Users\Duche\OneDrive\GitHub\dfbnb-data\dist\titles_images_manifest.json"

if (!(Test-Path $TsvPath)) { throw "TSV not found: $TsvPath" }

# Read header and find column indexes
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

function NormSlashes([string]$p) {
  if ([string]::IsNullOrWhiteSpace($p)) { return "" }
  $p = $p.Trim().Replace("/", "\")
  $p = $p -replace "^[.\\]+", ""
  return $p
}

$items = New-Object System.Collections.Generic.List[object]

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

  # Combine ETIP + ETDI, then strip leading "Textures\"
  $dds = (NormSlashes $etip) + (NormSlashes $etdi)

  if ($dds.ToLowerInvariant().StartsWith("textures\")) {
    $dds = $dds.Substring(9)  # length of "Textures\"
  }

  $items.Add([PSCustomObject]@{
    entitlement = $ent
    dds         = $dds
  })
}

# Ensure output folder exists
$outDir = Split-Path -Parent $OutPath
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

# Write JSON
$items | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $OutPath -Encoding UTF8

Write-Host "OK: wrote $($items.Count) entries to $OutPath"