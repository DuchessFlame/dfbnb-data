param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("titles-camp","titles-player")]
    [string]$Target
)

$ErrorActionPreference = "Stop"

# Local upload source (single storage location)
$localBase = "C:\Users\Duche\OneDrive\Guides and Stuff\Json Files for Website\1 site-data\json\uploads\fo76\storefront"
$local = Join-Path $localBase $Target

if (-not (Test-Path -LiteralPath $local)) {
    Write-Host "Local folder not found: $local"
    exit 1
}

# WP Engine SFTP details
$sftpHost = "buffsnbrew1.sftp.wpengine.com"
$port = 2222
$user = "buffsnbrew1-nav"

# Remote folder
$remote = "/wp-content/uploads/storefront/$Target/"

# WinSCP.com path
$winscp = "D:\WinSCP\WinSCP.com"
if (-not (Test-Path -LiteralPath $winscp)) {
    Write-Host "WinSCP.com not found at: $winscp"
    exit 1
}

Write-Host ""
Write-Host "=== Storefront upload (WinSCP) ==="
Write-Host "Target: $Target"
Write-Host "Local:  $local"
Write-Host "Remote: $remote"
Write-Host "Host:   ${sftpHost}:$port"
Write-Host "User:   $user"
Write-Host ""

# WinSCP script file (password will be prompted in PowerShell, not stored long-term)
$scriptPath = Join-Path $env:TEMP ("winscp_storefront_" + $Target + ".txt")

# Prompt in PowerShell so there is no "WinSCP password prompt timeout" issue
$secure = Read-Host "Enter SFTP password for $user" -AsSecureString
$sftpPassword = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
  [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
)

$winScpScript = @"
option batch continue
option confirm off
open sftp://${user}@${sftpHost}:$port/ -password="$sftpPassword"

cd /wp-content/uploads/storefront/$Target

rm *.webp

option batch abort

lcd "$local"
put -nopreservetime *.webp
exit
"@

Set-Content -LiteralPath $scriptPath -Value $winScpScript -Encoding ASCII

# Run WinSCP (it will prompt for password)
& $winscp /script="$scriptPath"

Remove-Item -LiteralPath $scriptPath -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "=== Done ==="