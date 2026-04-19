param(
    [Parameter(Mandatory=$false)]
    [string]$Target = ""
)

<#
.SYNOPSIS
  SFTP sync for mini season reward images to WP Engine.
  Uploads .avif images to /wp-content/uploads/guide-images/mini-seasons/

.DESCRIPTION
  If -Target is specified, uploads from a subfolder (e.g. love-hurts, weapons-expert).
  If -Target is omitted, uploads all .avif files from the root mini-seasons image folder.

  NOTE: You'll need to create per-event subfolders locally and place the exported
  AVIF images there. The image filenames must match the EDIDs used in the JSON
  (e.g. score_miniseason_lovehurts_weapons_nitro_stock_mod_1.avif).

.USAGE
  .\sync_mini_season_images_to_site.ps1                           # all images from root
  .\sync_mini_season_images_to_site.ps1 -Target love-hurts        # only love-hurts subfolder
  .\sync_mini_season_images_to_site.ps1 -Target weapons-expert    # only weapons-expert subfolder
#>

$ErrorActionPreference = "Stop"

$localBase = "C:\Users\Duche\OneDrive\Guides and Stuff\Json Files for Website\1 site-data\json\uploads\guide-images\mini-seasons"

if ($Target) {
    $local = Join-Path $localBase $Target
} else {
    $local = $localBase
}

if (-not (Test-Path -LiteralPath $local)) {
    Write-Host "Local folder not found: $local"
    Write-Host "Create the folder and place your .avif images there first."
    exit 1
}

$sftpHost = "buffsnbrew1.sftp.wpengine.com"
$port = 2222
$user = "buffsnbrew1-nav"

$remoteBase = "/wp-content/uploads/guide-images/mini-seasons"

if ($Target) {
    $remote = "$remoteBase/$Target"
} else {
    $remote = $remoteBase
}

$winscp = "D:\WinSCP\WinSCP.com"
if (-not (Test-Path -LiteralPath $winscp)) {
    Write-Host "WinSCP.com not found at: $winscp"
    exit 1
}

# Count images
$imageCount = (Get-ChildItem -LiteralPath $local -Filter "*.avif" -ErrorAction SilentlyContinue).Count
if ($imageCount -eq 0) {
    Write-Host "No .avif images found in: $local"
    Write-Host "Nothing to upload."
    exit 0
}

Write-Host ""
Write-Host "=== Mini Season Images Upload (WinSCP) ==="
Write-Host "Target: $(if ($Target) { $Target } else { '(all)' })"
Write-Host "Local:  $local"
Write-Host "Remote: $remote"
Write-Host "Images: $imageCount .avif file(s)"
Write-Host ""

$scriptPath = Join-Path $env:TEMP ("winscp_mini_seasons_images.txt")

$secure = Read-Host "Enter SFTP password for $user" -AsSecureString
$sftpPassword = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
)

$winScpScript = @"
option batch continue
option confirm off
open sftp://${user}@${sftpHost}:$port/ -password="$sftpPassword"

cd $remote

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
