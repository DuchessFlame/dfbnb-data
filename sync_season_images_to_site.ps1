param(
  [Parameter(Mandatory=$true)]
  [ValidateSet("season-ticket","utility")]
  [string]$Target
)

$ErrorActionPreference = "Stop"

$localBase = "C:\Users\Duche\OneDrive\Guides and Stuff\Json Files for Website\1 site-data\json\uploads\fo76\season_images"
$local = Join-Path $localBase $Target

if (-not (Test-Path -LiteralPath $local)) {
  Write-Host "Local folder not found: $local"
  exit 1
}

$sftpHost = "buffsnbrew1.sftp.wpengine.com"
$port = 2222
$user = "buffsnbrew1-nav"

# Remote folder(s) that already exist on the server (FileZilla)
$remoteBase = "/wp-content/uploads/season_images"

# season-ticket uploads go directly into /season_images (no extra subfolder)
# utility uploads go into /season_images/utility
if ($Target -eq "utility") {
  $remote = $remoteBase + "/utility"
} else {
  $remote = $remoteBase
}

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

cd $remote

rm *.webp

option batch abort

lcd "$local"
put -nopreservetime *.webp
exit
"@

Set-Content -LiteralPath $scriptPath -Value $winScpScript -Encoding ASCII
& $winscp /script="$scriptPath"
Remove-Item -LiteralPath $scriptPath -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "=== Done ==="