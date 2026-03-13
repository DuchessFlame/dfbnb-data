param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("json","assets","all")]
    [string]$Target = "all"
)

<#
.SYNOPSIS
  SFTP sync for challenges data + theme assets to WP Engine.
  JSON comes from the repo dist/ folder.
  JS/CSS come from the local dfbnb-child/assets theme folder.

.USAGE
  .\sync_challenges_to_site.ps1                 # uploads everything
  .\sync_challenges_to_site.ps1 -Target json    # only challenges.json
  .\sync_challenges_to_site.ps1 -Target assets  # only JS + CSS
  .\sync_challenges_to_site.ps1 -Target all     # json + assets
#>

$ErrorActionPreference = "Stop"

# ── Local paths ──────────────────────────────────────────────────
# JSON from repo dist/
$RepoRoot = $PSScriptRoot
if (-not (Test-Path (Join-Path $RepoRoot ".git"))) {
    $RepoRoot = Split-Path -Parent $PSScriptRoot
}
$DistJSON = Join-Path $RepoRoot "dist\challenges\challenges.json"

# JS/CSS from local theme assets folder
$AssetsDir = "C:\Users\Duche\OneDrive\Guides and Stuff\Json Files for Website\1 site-data\json\dfbnb-child\assets"
$LocalCSS  = Join-Path $AssetsDir "df-bnb-challenges.css"
$LocalJS   = Join-Path $AssetsDir "df-bnb-challenges.js"

# WP Engine SFTP
$SftpHost = "buffsnbrew1.sftp.wpengine.com"
$Port     = 2222
$User     = "buffsnbrew1-nav"

# Remote destinations on WP Engine
$RemoteJSON   = "/wp-content/uploads/json/challenges/"
$RemoteAssets  = "/wp-content/themes/dfbnb-child/assets/"

# WinSCP
$WinSCP = "D:\WinSCP\WinSCP.com"
if (-not (Test-Path -LiteralPath $WinSCP)) {
    Write-Error "WinSCP.com not found at: $WinSCP"
    exit 1
}

# ── Validate local files ─────────────────────────────────────────
$UploadPairs = @()   # Each entry: @{ Local = "..."; Remote = "..." }

if ($Target -eq "json" -or $Target -eq "all") {
    if (-not (Test-Path -LiteralPath $DistJSON)) {
        Write-Host "challenges.json not found at: $DistJSON"
        Write-Host "Run build_challenges_json.py first."
        exit 1
    }
    $UploadPairs += @{ Local = $DistJSON; Remote = $RemoteJSON }
}

if ($Target -eq "assets" -or $Target -eq "all") {
    if (-not (Test-Path -LiteralPath $LocalCSS)) {
        Write-Host "df-bnb-challenges.css not found at: $LocalCSS"
        exit 1
    }
    if (-not (Test-Path -LiteralPath $LocalJS)) {
        Write-Host "df-bnb-challenges.js not found at: $LocalJS"
        exit 1
    }
    $UploadPairs += @{ Local = $LocalCSS; Remote = $RemoteAssets }
    $UploadPairs += @{ Local = $LocalJS;  Remote = $RemoteAssets }
}

Write-Host ""
Write-Host "=== Challenges Sync (WinSCP) ==="
Write-Host "Target:  $Target"
Write-Host "Host:    ${SftpHost}:$Port"
Write-Host "User:    $User"
Write-Host "Files:"
foreach ($pair in $UploadPairs) {
    $name = Split-Path -Leaf $pair.Local
    $size = [math]::Round((Get-Item $pair.Local).Length / 1KB, 1)
    Write-Host "  -> $name ($size KB) => $($pair.Remote)"
}
Write-Host ""

# ── SFTP password ────────────────────────────────────────────────
$Secure = Read-Host "Enter SFTP password for $User" -AsSecureString
$SftpPassword = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secure)
)

# ── Build WinSCP script ─────────────────────────────────────────
$ScriptPath = Join-Path $env:TEMP "winscp_challenges_sync.txt"

$PutLines = ""
$lastRemote = ""
foreach ($pair in $UploadPairs) {
    if ($pair.Remote -ne $lastRemote) {
        $PutLines += "cd $($pair.Remote)`n"
        $lastRemote = $pair.Remote
    }
    $PutLines += "put -nopreservetime `"$($pair.Local)`"`n"
}

$WinScpScript = @"
option batch continue
option confirm off
open sftp://${User}@${SftpHost}:$Port/ -password="$SftpPassword"

option batch abort

$PutLines
exit
"@

Set-Content -LiteralPath $ScriptPath -Value $WinScpScript -Encoding ASCII

& $WinSCP /script="$ScriptPath"
$ExitCode = $LASTEXITCODE

Remove-Item -LiteralPath $ScriptPath -ErrorAction SilentlyContinue

if ($ExitCode -ne 0) {
    Write-Host ""
    Write-Host "WinSCP exited with code $ExitCode — check output above for errors."
    exit $ExitCode
}

Write-Host ""
Write-Host "=== Done ==="
