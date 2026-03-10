param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("camp-allies","camp-pets","camp-utility")]
    [string]$Target
)

$ErrorActionPreference = "Stop"

$LocalBase = "C:\Users\Duche\OneDrive\Guides and Stuff\Json Files for Website\1 site-data\json\uploads\fo76\storefront"
$Local = Join-Path $LocalBase $Target

if (-not (Test-Path -LiteralPath $Local)) {
    Write-Host "Local folder not found: $Local"
    Write-Host "Run run_allies_pets_weather_images.ps1 first."
    exit 1
}

$SftpHost = "buffsnbrew1.sftp.wpengine.com"
$Port     = 2222
$User     = "buffsnbrew1-nav"
$RemoteFolder = "/wp-content/uploads/storefront/" + $Target + "/"

$WinSCP = "D:\WinSCP\WinSCP.com"
if (-not (Test-Path -LiteralPath $WinSCP)) {
    Write-Error "WinSCP.com not found at: $WinSCP"
    exit 1
}

Write-Host ""
Write-Host "=== Allies / Pets / Weather upload ==="
Write-Host "Target: $Target"
Write-Host "Local:  $Local"
Write-Host "Remote: $RemoteFolder"
Write-Host ""

$ScriptPath = Join-Path $env:TEMP ("winscp_apw_" + $Target + ".txt")

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
    "rm *.webp",
    "",
    "option batch abort",
    "",
    ("lcd `"" + $Local + "`""),
    "put -nopreservetime *.webp",
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
