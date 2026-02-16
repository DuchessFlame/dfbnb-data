# sync_storefront_to_site.ps1
# Upload local storefront webps to /wp-content/uploads/storefront using WinSCP.

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$local  = "C:\Users\Duche\OneDrive\Guides and Stuff\Json Files for Website\1 site-data\json\uploads\fo76\storefront"
# Locate WinSCP.com (auto-detect common install paths)
$winscpCandidates = @(
  "C:\Program Files (x86)\WinSCP\WinSCP.com",
  "C:\Program Files\WinSCP\WinSCP.com"
)

$winscp = $winscpCandidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1

if (-not $winscp) {
  throw "WinSCP.com not found. Install WinSCP, or set `$winscp to the correct full path."
}
$remote = "/wp-content/uploads/storefront"

# EDIT THESE
$sftpHost = "YOUR_SFTP_HOST"
$port = 22
$user = "YOUR_SFTP_USER"
$pass = "YOUR_SFTP_PASSWORD"

function Assert-Path($p, $label) {
  if (-not (Test-Path -LiteralPath $p)) { throw "$label not found: $p" }
}

Assert-Path $local  "Local storefront folder"
Assert-Path $winscp "WinSCP.com"

$scriptPath = Join-Path $env:TEMP "winscp_storefront_sync.txt"
$logPath    = Join-Path $env:TEMP "winscp_storefront_sync.log"

Write-Host "=== Storefront sync ==="
Write-Host "Local:  $local"
Write-Host "Remote: $remote"
Write-Host "Host:   $($sftpHost):$($port)"
Write-Host ""

@"
option batch on
option confirm off
open sftp://$user`:$pass@$sftpHost`:$port/
synchronize remote "$local" "$remote"
exit
"@ | Set-Content -Encoding ASCII -LiteralPath $scriptPath

& $winscp "/script=$scriptPath" "/log=$logPath"

Write-Host ""
Write-Host "WinSCP log: $logPath"
Write-Host "=== Done ==="