<#
sync_curves_to_site.ps1
Uploads dfbnb-data\dist\curves\* to:
  /wp-content/uploads/curves/

Uses WinSCP.com via SFTP (WP Engine).
Prompts for password in PowerShell (not stored).
#>

[CmdletBinding()]
param(
  # Optional override if your dist folder is elsewhere
  [string]$LocalCurvesDir = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Resolve dfbnb-data repo root (this script should live in dfbnb-data root)
$DfRepo = $PSScriptRoot

# Default local dist folder
if ([string]::IsNullOrWhiteSpace($LocalCurvesDir)) {
  $LocalCurvesDir = Join-Path $DfRepo "dist\curves"
}

if (-not (Test-Path -LiteralPath $LocalCurvesDir)) {
  Write-Host "Local curves folder not found: $LocalCurvesDir"
  exit 1
}

# WP Engine SFTP details
$sftpHost = "buffsnbrew1.sftp.wpengine.com"
$port = 2222
$user = "buffsnbrew1-nav"

# Remote folder
$remoteDir = "/wp-content/uploads/curves"

# WinSCP.com path (match your existing scripts)
$winscp = "D:\WinSCP\WinSCP.com"
if (-not (Test-Path -LiteralPath $winscp)) {
  Write-Host "WinSCP.com not found at: $winscp"
  exit 1
}

Write-Host ""
Write-Host "=== Curves upload (WinSCP) ==="
Write-Host "Local:  $LocalCurvesDir"
Write-Host "Remote: $remoteDir/"
Write-Host "Host:   ${sftpHost}:$port"
Write-Host "User:   $user"
Write-Host ""

# Prompt for password (not stored)
$secure = Read-Host "Enter SFTP password for $user" -AsSecureString
$sftpPassword = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
  [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
)

# Temp WinSCP script file
$scriptPath = Join-Path $env:TEMP ("winscp_curves_upload_" + [Guid]::NewGuid().ToString("N") + ".txt")

# Use WinSCP "synchronize remote" to mirror local -> remote (uploads + deletes removed files)
$winScpScript = @"
option batch abort
option confirm off
open sftp://${user}@${sftpHost}:$port/ -password="$sftpPassword"

cd $remoteDir

# Upload everything recursively, binary mode, do NOT preserve timestamps,
# delete remote files that no longer exist locally
put -transfer=binary -nopreservetime -delete -filemask="*" -resume "$LocalCurvesDir\*" "$remoteDir/"

exit
"@

Set-Content -LiteralPath $scriptPath -Value $winScpScript -Encoding ASCII

& $winscp /script="$scriptPath"

Remove-Item -LiteralPath $scriptPath -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "=== Done ==="