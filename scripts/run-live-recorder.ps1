param(
    [int]$DurationMin = 600,
    [int]$SnapshotIntervalS = 180,
    [ValidateSet("lean", "full")]
    [string]$StreamProfile = "lean",
    [string]$OutDir = ""
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir
Set-Location $RepoRoot

if ([string]::IsNullOrWhiteSpace($OutDir)) {
    $Stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $OutDir = Join-Path $RepoRoot "data\live_recordings\console_$Stamp"
}

$Addresses = @(
    "0xecb63caa47c7c4e77f60f1ce858cf28dc2b82b00",
    "0x023a3d058020fb76cca98f01b3c48c8938a22355",
    "0x7c930969fcf3e5a5c78bcf2e1cefda3f53e3c8fd",
    "0x399965e15d4e61ec3529cc98b7f7ebb93b733336",
    "0xf5d81a135f756ca16544e53c20fc20643ec3ad53",
    "0x0526345bf8e09eb32256008c2844c8949ee3bb9a"
)

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$Python = (Get-Command python).Source
$RecorderArgs = @("scripts\live_recorder.py")
foreach ($Address in $Addresses) {
    $RecorderArgs += @("--address", $Address)
}
$RecorderArgs += @(
    "--duration-min", "$DurationMin",
    "--snapshot-interval-s", "$SnapshotIntervalS",
    "--stream-profile", $StreamProfile,
    "--out-dir", $OutDir
)

Write-Host "Starting Hyperliquid live recorder in foreground."
Write-Host "Output: $OutDir"
Write-Host "Metrics: $(Join-Path $OutDir 'metrics.json')"
Write-Host "Stop: press Ctrl+C in this console."
Write-Host ""

& $Python @RecorderArgs
