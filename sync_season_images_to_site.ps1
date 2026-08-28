<#
.SYNOPSIS
  Uploads season reward images to WP Engine.

.DESCRIPTION
  Three upload targets:

    -Target utility        -> /season_images/utility/      (shared currency + consumable icons)
    -Target season-ticket  -> /season_images/              (legacy flat folder)
    -Season {N}            -> /season_images/season-{N}/   (per-season folder)

  The per-season folder is what the site actually reads. df-bnb-seasons.js
  resolveImageUrl() rewrites every reward imageUrl of the form
      /season_images/score_s{N}_*.webp
  to
      /season_images/season-{N}/score_s{N}_*.avif
  so the JSON never needs to change when images land - only the upload target.

.EXAMPLE
  .\sync_season_images_to_site.ps1 -Season 12
  .\sync_season_images_to_site.ps1 -Target utility
#>
[CmdletBinding(DefaultParameterSetName = "ByTarget")]
param(
  [Parameter(Mandatory=$true, ParameterSetName="ByTarget")]
  [ValidateSet("season-ticket","utility")]
  [string]$Target,

  # Per-season upload: stages from season_images\season-{N}\ to /season_images/season-{N}/
  [Parameter(Mandatory=$true, ParameterSetName="BySeason")]
  [ValidateRange(1,99)]
  [int]$Season
)

$ErrorActionPreference = "Stop"

$localBase = "C:\Users\Duche\OneDrive\Guides and Stuff\Json Files for Website\1 site-data\json\uploads\fo76\season_images"
$remoteBase = "/wp-content/uploads/season_images"

if ($PSCmdlet.ParameterSetName -eq "BySeason") {
  $Target = "season-$Season"
  $local  = Join-Path $localBase $Target
  $remote = "$remoteBase/$Target"
} elseif ($Target -eq "utility") {
  $local  = Join-Path $localBase $Target
  $remote = "$remoteBase/utility"
} else {
  $local  = Join-Path $localBase $Target
  $remote = $remoteBase
}

if (-not (Test-Path -LiteralPath $local)) {
  Write-Host "Local folder not found: $local"
  if ($PSCmdlet.ParameterSetName -eq "BySeason") {
    Write-Host ""
    Write-Host "Stage the converted AVIFs there first, e.g.:"
    Write-Host "  python tools\convert_season_images_to_avif.py --src `"...\.Season Images\Season $Season`" --season $Season"
    Write-Host "  then copy avif\season-$Season\*.avif into:"
    Write-Host "  $local"
  }
  exit 1
}

$avifCount = @(Get-ChildItem -LiteralPath $local -Filter *.avif -File -ErrorAction SilentlyContinue).Count
if ($avifCount -eq 0) {
  Write-Host "No .avif files in: $local"
  Write-Host "Nothing to upload - aborting so the remote folder is not wiped."
  exit 1
}

$sftpHost = "buffsnbrew1.sftp.wpengine.com"
$port = 2222
$user = "buffsnbrew1-nav"

$winscp = "D:\WinSCP\WinSCP.com"
if (-not (Test-Path -LiteralPath $winscp)) {
  Write-Host "WinSCP.com not found at: $winscp"
  exit 1
}

Write-Host ""
Write-Host "=== Season Images upload (WinSCP) ==="
Write-Host "Target: $Target"
Write-Host "Local:  $local"
Write-Host "Remote: $remote"
Write-Host "Files:  $avifCount .avif"
Write-Host ""

$scriptPath = Join-Path $env:TEMP ("winscp_season_images_" + $Target + ".txt")

$secure = Read-Host "Enter SFTP password for $user" -AsSecureString
$sftpPassword = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
  [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
)

$winScpScript = @"
option batch continue
option confirm off
open sftp://${user}@${sftpHost}:$port/ -password="$sftpPassword"

# Per-season folders may not exist yet on a first upload. mkdir is expected to
# fail harmlessly when the folder is already there (batch mode is 'continue' here).
mkdir $remote

cd $remote

# Remove old WebP files (transition cleanup - harmless once all files are AVIF)
rm *.webp

# Remove existing AVIF files before uploading fresh set
rm *.avif

option batch abort

lcd "$local"
put -nopreservetime *.avif
exit
"@

Set-Content -LiteralPath $scriptPath -Value $winScpScript -Encoding ASCII
& $winscp /script="$scriptPath"
Remove-Item -LiteralPath $scriptPath -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "=== Done ==="
