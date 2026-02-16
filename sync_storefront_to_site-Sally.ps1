Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$localDir = "C:\Users\Duche\OneDrive\Guides and Stuff\Json Files for Website\1 site-data\json\uploads\fo76\storefront"

$sftpHost = "buffsnbrew1.sftp.wpengine.com"
$sftpUser = "buffsnbrew1-nav"
$sftpPort = 2222

if (-not (Test-Path -LiteralPath $localDir)) {
  throw "Local folder not found: $localDir"
}

$webpCount = (Get-ChildItem -LiteralPath $localDir -File -Filter "*.webp" -ErrorAction SilentlyContinue | Measure-Object).Count
Write-Host "Local storefront WebPs: $webpCount"
if ($webpCount -eq 0) {
  throw "No .webp files found in: $localDir"
}

function Invoke-SftpBatch {
  param(
    [Parameter(Mandatory=$true)][string]$remoteDir
  )

  $batch = Join-Path $env:TEMP ("dfbnb_storefront_upload_" + [Guid]::NewGuid().ToString("N") + ".txt")

  @"
pwd
mkdir $remoteDir
cd $remoteDir
pwd
ls
lcd $localDir
mput *.webp
ls
bye
"@ | Set-Content -LiteralPath $batch -Encoding ASCII

  try {
    Write-Host ""
    Write-Host "Attempting upload to: $remoteDir"
    Write-Host ""
    sftp -oStrictHostKeyChecking=accept-new -P $sftpPort -b $batch "$sftpUser@$sftpHost"
    return $LASTEXITCODE
  }
  finally {
    Remove-Item -LiteralPath $batch -Force -ErrorAction SilentlyContinue
  }
}

# Attempt 1: relative path (most common with chroot)
$code = Invoke-SftpBatch -remoteDir "wp-content/uploads/storefront"
if ($code -eq 0) {
  Write-Host ""
  Write-Host "Upload complete (wp-content/uploads/storefront)."
  exit 0
}

# Attempt 2: some WP Engine accounts expose /sites/<install>/
$code = Invoke-SftpBatch -remoteDir "sites/buffsnbrew1/wp-content/uploads/storefront"
if ($code -eq 0) {
  Write-Host ""
  Write-Host "Upload complete (sites/buffsnbrew1/wp-content/uploads/storefront)."
  exit 0
}

throw "SFTP upload failed for both target paths. See output above for the remote root (pwd) and errors."