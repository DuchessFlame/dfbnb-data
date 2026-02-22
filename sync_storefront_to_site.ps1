# sync_storefront_to_site.ps1
# Upload local storefront webps to /wp-content/uploads/storefront using Windows OpenSSH sftp.exe

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Local build output (must match run_storefront_build.ps1 output)
$local = "C:\Users\Duche\OneDrive\Guides and Stuff\Json Files for Website\1 site-data\json\uploads\fo76\storefront"

# Remote target
$remote = "/wp-content/uploads/storefront"

# WP Engine SFTP details
$sftpHost = "buffsnbrew1.sftp.wpengine.com"
$port     = 2222
$user     = "buffsnbrew1-nav"

function Assert-Path($p, $label) {
  if (-not (Test-Path -LiteralPath $p)) { throw "$label not found: $p" }
}

Assert-Path $local "Local storefront folder"

# Ensure sftp.exe exists (Windows OpenSSH Client feature)
$sftpCmd = Get-Command sftp.exe -ErrorAction SilentlyContinue
$sftpExe = if ($sftpCmd) { $sftpCmd.Source } else { $null }
if (-not $sftpExe) {
  throw "sftp.exe not found. Install Windows optional feature: OpenSSH Client."
}

Write-Host "=== Storefront sync (sftp.exe) ==="
Write-Host "Local:  $local"
Write-Host "Remote: $remote"
Write-Host "Host: ${sftpHost}:$port"
Write-Host "User:   $user"
Write-Host ""

# Build SFTP batch commands
$batchPath = Join-Path $env:TEMP ("sftp_storefront_sync_" + [Guid]::NewGuid().ToString("N") + ".txt")

@"
mkdir $remote
cd $remote
lcd "$local"
mput *.webp
bye
"@ | Set-Content -Encoding ASCII -LiteralPath $batchPath

# Run SFTP. This will prompt for your password unless you have key auth set up.
& $sftpExe -P $port -b $batchPath "$user@$sftpHost"

Write-Host ""
Write-Host "=== Done ==="