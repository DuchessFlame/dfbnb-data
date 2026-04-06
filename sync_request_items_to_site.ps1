<#
.SYNOPSIS
  SFTP sync for Atom Shop request-item images to WP Engine.
  Called by run_request_item_images.ps1 after building AVIFs,
  or run standalone to re-upload.

.USAGE
  .\sync_request_items_to_site.ps1
#>

$ErrorActionPreference = "Stop"

# ---- Paths ----
$LocalBase = "C:\Users\Duche\OneDrive\Guides and Stuff\Json Files for Website\1 site-data\json\uploads\fo76\storefront"
$Local     = Join-Path $LocalBase "request-item-images"

if (-not (Test-Path -LiteralPath $Local)) {
    Write-Host "Local folder not found: $Local"
    Write-Host "Run run_request_item_images.ps1 first."
    exit 1
}

# ---- WP Engine SFTP details ----
$SftpHost     = "buffsnbrew1.sftp.wpengine.com"
$Port         = 2222
$User         = "buffsnbrew1-nav"
$RemoteFolder = "/wp-content/uploads/guide-images/atom-shop/request-item-images/"

$WinSCP = "D:\WinSCP\WinSCP.com"
if (-not (Test-Path -LiteralPath $WinSCP)) {
    Write-Error "WinSCP.com not found at: $WinSCP"
    exit 1
}

Write-Host ""
Write-Host "=== Request Item Images upload (WinSCP) ==="
Write-Host "Local:  $Local"
Write-Host "Remote: $RemoteFolder"
Write-Host "Host:   ${SftpHost}:$Port"
Write-Host "User:   $User"
Write-Host ""

$ScriptPath = Join-Path $env:TEMP "winscp_request_items.txt"

# ---- Prompt for password ----
$Secure = Read-Host "Enter SFTP password for $User" -AsSecureString
$SftpPassword = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secure)
)

$lines = @(
    "option batch continue",
    "option confirm off",
    ("open sftp://" + $User + "@" + $SftpHost + ":" + $Port + "/ -password=`"" + $SftpPassword + "`""),
    "",
    ("cd " + $RemoteFolder),
    "",
    "# Remove existing AVIF files before uploading fresh set",
    "rm *.avif",
    "",
    "option batch abort",
    "",
    ("lcd `"" + $Local + "`""),
    "put -nopreservetime *.avif",
    "exit"
)

Set-Content -LiteralPath $ScriptPath -Value ($lines -join "`r`n") -Encoding ASCII

$ErrorActionPreference = "Continue"
& $WinSCP /script="$ScriptPath"
$ExitCode = $LASTEXITCODE
$ErrorActionPreference = "Stop"

Remove-Item -LiteralPath $ScriptPath -ErrorAction SilentlyContinue

if ($ExitCode -ne 0) {
    Write-Host ""
    Write-Host "WinSCP exited with code $ExitCode - check output above for errors."
    exit $ExitCode
}

Write-Host ""
Write-Host "=== Done ==="
