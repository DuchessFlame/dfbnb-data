<#
.SYNOPSIS
  Uploads a season's GALLERY images (community calendar + board pages) to WP Engine.

.DESCRIPTION
  Target: /wp-content/uploads/season_images/season-{N}/AVIF/

  This is deliberately a SEPARATE folder and a SEPARATE script from
  sync_season_images_to_site.ps1. That script does `rm *.avif` on
  /season_images/season-{N}/ before uploading, so anything staged there that
  is not part of the reward-icon set gets wiped on the next icon sync. Keeping
  the gallery one level down in AVIF/ means the two can never clobber each
  other.

  The folder holds the .avif images plus a gallery.json manifest. df-bnb-seasons.js
  reads the manifest, so no code or JSON change is needed for a new season -
  only these files. A season with no manifest on the server renders no gallery,
  and an image listed in the manifest but missing removes its own thumbnail.

.EXAMPLE
  # 1. build the AVIFs
  python tools\build_season_gallery_avif.py --season 26 --stage
  # 2. upload them
  .\sync_season_gallery_to_site.ps1 -Season 26
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)]
  [ValidateRange(1,99)]
  [int]$Season
)

$ErrorActionPreference = "Stop"

$localBase = "C:\Users\Duche\OneDrive\Guides and Stuff\Json Files for Website\1 site-data\json\uploads\fo76\season_images"
$local  = Join-Path $localBase ("season-$Season\AVIF")
$remote = "/wp-content/uploads/season_images/season-$Season/AVIF"

if (-not (Test-Path -LiteralPath $local)) {
  Write-Host "Local folder not found: $local"
  Write-Host ""
  Write-Host "Build and stage the gallery AVIFs first:"
  Write-Host "  python tools\build_season_gallery_avif.py --season $Season --stage"
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
Write-Host "=== Season $Season gallery upload (WinSCP) ==="
Write-Host "Local:  $local"
Write-Host "Remote: $remote"
Write-Host "Files:  $avifCount .avif"
Write-Host ""

$scriptPath = Join-Path $env:TEMP ("winscp_season_gallery_$Season.txt")

$secure = Read-Host "Enter SFTP password for $user" -AsSecureString
$sftpPassword = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
  [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
)

$winScpScript = @"
option batch continue
option confirm off
open sftp://${user}@${sftpHost}:$port/ -password="$sftpPassword"

# Both levels may be missing on a first upload for this season. mkdir failing
# because the folder already exists is expected and harmless under 'continue'.
mkdir /wp-content/uploads/season_images/season-$Season
mkdir $remote

cd $remote

# Replace the previous set. Scoped to this AVIF subfolder only - the reward
# icons in the parent season-$Season folder are never touched.
rm *.avif
rm gallery.json

option batch abort

lcd "$local"
put -nopreservetime *.avif
put -nopreservetime gallery.json
exit
"@

Set-Content -LiteralPath $scriptPath -Value $winScpScript -Encoding ASCII
& $winscp /script="$scriptPath"
Remove-Item -LiteralPath $scriptPath -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "=== Done ==="
Write-Host "Check: https://www.buffsnbrew.com/df/scoreboards/season-$Season/scoreboard/"
