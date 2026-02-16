# run_storefront_build_and_upload.ps1
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

& (Join-Path $PSScriptRoot "run_storefront_build.ps1")
& (Join-Path $PSScriptRoot "sync_storefront_to_site.ps1")