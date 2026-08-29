<#
.SYNOPSIS
  SFTP sync for Player Icon images to WP Engine.
  Uploads the 649 converted AVIFs to the new canonical folder used by both
  /df/atom-shop/player-icons/ and every scoreboard player-icon reward.

.NOTES
  Unlike sync_bundles_to_site.ps1 this does NOT `rm *.avif` on the remote first,
  and it copies the old folder across before uploading.

  Two icons the scoreboards reference — atx_playericon_playericon1.avif and
  score_s9_playericon_dreadislandinverse.avif — exist ONLY in the old
  /wp-content/uploads/storefront/player-icons/ folder, because their .dds was
  never in the texture extract. If the new folder is ever wiped, or seeding is
  skipped, those two season rows go back to broken images. That is why the seed
  is on by default and why nothing is deleted remotely.

.USAGE
  .\sync_player_icons_to_site.ps1
  .\sync_player_icons_to_site.ps1 -NoSeed    # skip the old-folder copy
#>

param(
    # Skip the server-side copy of the old storefront/player-icons folder.
    # Only safe once you have confirmed the new folder already holds the two
    # icons listed in .NOTES.
    [switch]$NoSeed
)

$SeedFromOld = -not $NoSeed

$ErrorActionPreference = "Stop"

# ---- Paths ----
$Local = "C:\Users\Duche\OneDrive\Guides and Stuff\.Atom Shop\Player Icons"

if (-not (Test-Path -LiteralPath $Local)) {
    Write-Host "Local folder not found: $Local"
    exit 1
}

$Count = (Get-ChildItem -LiteralPath $Local -Filter *.avif -File).Count
if ($Count -eq 0) {
    Write-Host "No .avif files in $Local - nothing to upload."
    exit 1
}

# ---- WP Engine SFTP details ----
$SftpHost     = "buffsnbrew1.sftp.wpengine.com"
$Port         = 2222
$User         = "buffsnbrew1-nav"
$RemoteFolder = "/wp-content/uploads/guide-images/atom-shop/player-icons/"
$OldFolder    = "/wp-content/uploads/storefront/player-icons/"

$WinSCP = "D:\WinSCP\WinSCP.com"
if (-not (Test-Path -LiteralPath $WinSCP)) {
    Write-Error "WinSCP.com not found at: $WinSCP"
    exit 1
}

Write-Host ""
Write-Host "=== Player Icons upload (WinSCP) ==="
Write-Host "Local:  $Local  ($Count avif)"
Write-Host "Remote: $RemoteFolder"
Write-Host "Host:   ${SftpHost}:$Port"
Write-Host "User:   $User"
if ($SeedFromOld) { Write-Host "Seed:   copying $OldFolder first" }
Write-Host ""

$ScriptPath = Join-Path $env:TEMP "winscp_player_icons.txt"

# ---- Prompt for password ----
$Secure = Read-Host "Enter SFTP password for $User" -AsSecureString
$SftpPassword = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secure)
)

$lines = New-Object System.Collections.Generic.List[string]
$lines.Add("option batch continue")
$lines.Add("option confirm off")
$lines.Add("open sftp://" + $User + "@" + $SftpHost + ":" + $Port + "/ -password=`"" + $SftpPassword + "`"")
$lines.Add("")

if ($SeedFromOld) {
    # Server-side copy so nothing that already worked regresses. Anything the
    # fresh upload also provides is overwritten a moment later.
    $lines.Add("# Seed the new folder from the old one (first run only)")
    $lines.Add("cp " + $OldFolder + "*.avif " + $RemoteFolder)
    $lines.Add("")
}

$lines.Add("option batch abort")
$lines.Add("")
$lines.Add("cd " + $RemoteFolder)
$lines.Add("lcd `"" + $Local + "`"")
$lines.Add("put -nopreservetime *.avif")
$lines.Add("exit")

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
Write-Host "=== Done - $Count player icons uploaded ==="
