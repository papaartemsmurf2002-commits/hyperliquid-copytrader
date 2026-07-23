param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern("^0x[0-9a-fA-F]{40}$")]
    [string]$SourceWallet,
    [Parameter(Mandatory = $true)]
    [ValidateSet("mainnet", "testnet")]
    [string]$SourceNetwork,
    [Parameter(Mandatory = $true)]
    [ValidatePattern("^0x[0-9a-fA-F]{40}$")]
    [string]$FollowerAddress,
    [Parameter(Mandatory = $true)]
    [ValidatePattern("^0x[0-9a-fA-F]{40}$")]
    [string]$ApiWalletAddress,
    [string]$VaultAddress = "",
    [string]$Coin = "BTC",
    [string]$Size = "0.0001",
    [string]$MaxNotionalUsd = "25",
    [string]$MaxGrossExposureUsd = "100",
    [int]$MaxLeverage = 3,
    [ValidateSet("auto", "standard", "unified")]
    [string]$ExpectedAccountMode = "auto",
    [string]$ApiPrivateKeyFile = ".secrets\testnet-api-private-key.txt",
    [string]$DbPath = "data\testnet-canary\copytrader.sqlite3",
    [string]$KillSwitchPath = "data\testnet-canary\KILL_SWITCH",
    [string]$CliPath = "",
    [switch]$SkipSmoke,
    [switch]$ActiveSmoke
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$localCli = Join-Path $repoRoot ".venv\Scripts\hl-copytrader.exe"
$cli = if (-not [string]::IsNullOrWhiteSpace($CliPath)) {
    $candidateCli = if ([IO.Path]::IsPathRooted($CliPath)) {
        [IO.Path]::GetFullPath($CliPath)
    }
    else {
        [IO.Path]::GetFullPath((Join-Path $repoRoot $CliPath))
    }
    if (-not (Test-Path -LiteralPath $candidateCli -PathType Leaf)) {
        throw "CLI executable does not exist: $candidateCli"
    }
    $candidateCli
}
elseif (Test-Path -LiteralPath $localCli) {
    $localCli
}
else {
    "hl-copytrader"
}
$envNames = @(
    "HLCT_MODE",
    "HLCT_SOURCE_WALLET",
    "HLCT_SOURCE_NETWORK",
    "HLCT_DB_PATH",
    "HLCT_KILL_SWITCH_PATH",
    "HLCT_FOLLOWER_ACCOUNT_ADDRESS",
    "HLCT_API_WALLET_ADDRESS",
    "HLCT_API_PRIVATE_KEY",
    "HLCT_API_PRIVATE_KEY_FILE",
    "HLCT_EXPECTED_ACCOUNT_MODE",
    "HLCT_VAULT_ADDRESS",
    "HLCT_LIVE_ENABLE",
    "HLCT_CONFIRM_MAINNET_LIVE",
    "HLCT_MAX_NOTIONAL_USD",
    "HLCT_MAX_GROSS_EXPOSURE_USD",
    "HLCT_MAX_LEVERAGE",
    "HLCT_ALLOW_MASTER_PRIVATE_KEY"
)
$oldEnv = @{}

function Save-Env {
    foreach ($name in $envNames) {
        $oldEnv[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
    }
}

function Restore-Env {
    foreach ($name in $envNames) {
        [Environment]::SetEnvironmentVariable($name, $oldEnv[$name], "Process")
    }
}

function Set-TestnetEnv {
    param(
        [string]$PrivateKeyFile,
        [string]$ResolvedDbPath,
        [string]$ResolvedKillSwitchPath
    )
    [Environment]::SetEnvironmentVariable("HLCT_MODE", "testnet", "Process")
    [Environment]::SetEnvironmentVariable("HLCT_SOURCE_WALLET", $SourceWallet, "Process")
    [Environment]::SetEnvironmentVariable("HLCT_SOURCE_NETWORK", $SourceNetwork, "Process")
    [Environment]::SetEnvironmentVariable("HLCT_DB_PATH", $ResolvedDbPath, "Process")
    [Environment]::SetEnvironmentVariable("HLCT_KILL_SWITCH_PATH", $ResolvedKillSwitchPath, "Process")
    [Environment]::SetEnvironmentVariable("HLCT_FOLLOWER_ACCOUNT_ADDRESS", $FollowerAddress, "Process")
    [Environment]::SetEnvironmentVariable("HLCT_API_WALLET_ADDRESS", $ApiWalletAddress, "Process")
    [Environment]::SetEnvironmentVariable("HLCT_API_PRIVATE_KEY", "", "Process")
    [Environment]::SetEnvironmentVariable("HLCT_API_PRIVATE_KEY_FILE", $PrivateKeyFile, "Process")
    [Environment]::SetEnvironmentVariable("HLCT_EXPECTED_ACCOUNT_MODE", $ExpectedAccountMode, "Process")
    [Environment]::SetEnvironmentVariable("HLCT_VAULT_ADDRESS", $VaultAddress, "Process")
    [Environment]::SetEnvironmentVariable("HLCT_LIVE_ENABLE", "false", "Process")
    [Environment]::SetEnvironmentVariable("HLCT_CONFIRM_MAINNET_LIVE", "false", "Process")
    [Environment]::SetEnvironmentVariable("HLCT_MAX_NOTIONAL_USD", $MaxNotionalUsd, "Process")
    [Environment]::SetEnvironmentVariable("HLCT_MAX_GROSS_EXPOSURE_USD", $MaxGrossExposureUsd, "Process")
    [Environment]::SetEnvironmentVariable("HLCT_MAX_LEVERAGE", [string]$MaxLeverage, "Process")
    [Environment]::SetEnvironmentVariable("HLCT_ALLOW_MASTER_PRIVATE_KEY", "false", "Process")
}

function Invoke-CopyTrader {
    param([string[]]$Arguments)
    Write-Host ""
    Write-Host "==> hl-copytrader $($Arguments -join ' ')"
    $output = & $cli @Arguments 2>&1
    $exitCode = $LASTEXITCODE
    $output | ForEach-Object { Write-Host $_ }
    if ($exitCode -ne 0) {
        throw "hl-copytrader $($Arguments -join ' ') failed with exit code $exitCode"
    }
    $json = ($output | Out-String).Trim()
    if ([string]::IsNullOrWhiteSpace($json)) {
        return $null
    }
    try {
        return $json | ConvertFrom-Json
    }
    catch {
        throw "hl-copytrader $($Arguments -join ' ') did not return parseable JSON: $($_.Exception.Message)"
    }
}

function Assert-Canary {
    param(
        [bool]$Condition,
        [string]$Message
    )
    if (-not $Condition) {
        throw $Message
    }
}

Push-Location $repoRoot
Save-Env
try {
    if ($SkipSmoke -and $ActiveSmoke) {
        throw "-SkipSmoke and -ActiveSmoke cannot be used together"
    }

    $keyPath = if ([IO.Path]::IsPathRooted($ApiPrivateKeyFile)) {
        $ApiPrivateKeyFile
    } else {
        Join-Path $repoRoot $ApiPrivateKeyFile
    }
    if (-not (Test-Path -LiteralPath $keyPath -PathType Leaf)) {
        throw "API private key file does not exist: $keyPath"
    }

    $resolvedDbPath = if ([IO.Path]::IsPathRooted($DbPath)) {
        [IO.Path]::GetFullPath($DbPath)
    } else {
        [IO.Path]::GetFullPath((Join-Path $repoRoot $DbPath))
    }
    $resolvedKillSwitchPath = if ([IO.Path]::IsPathRooted($KillSwitchPath)) {
        [IO.Path]::GetFullPath($KillSwitchPath)
    } else {
        [IO.Path]::GetFullPath((Join-Path $repoRoot $KillSwitchPath))
    }
    $stateDirectory = Split-Path -Parent $resolvedDbPath
    New-Item -ItemType Directory -Path $stateDirectory -Force | Out-Null

    Set-TestnetEnv -PrivateKeyFile $keyPath -ResolvedDbPath $resolvedDbPath -ResolvedKillSwitchPath $resolvedKillSwitchPath

    Write-Host "Using API private key file: $keyPath"
    Write-Host "Using dedicated canary journal: $resolvedDbPath"
    Write-Host "Using absolute canary kill switch: $resolvedKillSwitchPath"
    Write-Host "Running testnet preflight/auth probe and read-only source/follower truth refresh."
    $preflight = Invoke-CopyTrader -Arguments @("preflight", "--mode", "testnet")
    Assert-Canary -Condition ($preflight.passed -eq $true) -Message "testnet preflight did not pass"
    $refresh = Invoke-CopyTrader -Arguments @("refresh-readiness-truth", "--mode", "testnet")
    Assert-Canary -Condition ($refresh.passed -eq $true) -Message "read-only readiness truth refresh did not pass"
    $readiness = Invoke-CopyTrader -Arguments @("readiness", "--mode", "testnet")
    Assert-Canary -Condition ($readiness.ready -eq $true) -Message "testnet readiness did not pass"
    $verify = Invoke-CopyTrader -Arguments @("verify", "--mode", "testnet")
    Assert-Canary -Condition ($verify.preflight.passed -eq $true) -Message "testnet verify preflight did not pass"
    Assert-Canary -Condition ($verify.readiness.ready -eq $true) -Message "testnet verify readiness did not pass"

    if ($SkipSmoke) {
        Write-Host ""
        Write-Host "Smoke order skipped by -SkipSmoke."
        return
    }

    Write-Host ""
    Write-Host "Running automated testnet smoke: placing and canceling one passive testnet $Coin order."
    $smoke = Invoke-CopyTrader -Arguments @("testnet-smoke", "--coin", $Coin, "--size", $Size)
    Assert-Canary -Condition ($smoke.safe_mode.enabled -ne $true) -Message "testnet smoke ended in safe mode"
    Assert-Canary -Condition ($null -ne $smoke.place) -Message "testnet smoke did not attempt placement"
    Assert-Canary -Condition ($smoke.place.status -in @("acked", "filled")) -Message "testnet smoke placement did not ack or fill"
    Assert-Canary -Condition ($null -ne $smoke.cancel) -Message "testnet smoke did not attempt cancel"
    Assert-Canary -Condition ($smoke.cancel.status -eq "canceled") -Message "testnet smoke cancel did not succeed"
    $settlement = Invoke-CopyTrader -Arguments @("settle-pending", "--mode", "testnet")
    Assert-Canary -Condition ([int]$settlement.pending_after -eq 0) -Message "pending intents remain after settlement"
    $finalRefresh = Invoke-CopyTrader -Arguments @("refresh-readiness-truth", "--mode", "testnet")
    Assert-Canary -Condition ($finalRefresh.passed -eq $true) -Message "final readiness truth refresh did not pass"
    $finalVerify = Invoke-CopyTrader -Arguments @("verify", "--mode", "testnet")
    Assert-Canary -Condition ($finalVerify.preflight.passed -eq $true) -Message "final verify preflight did not pass"
    Assert-Canary -Condition ($finalVerify.readiness.ready -eq $true) -Message "final verify readiness did not pass"

    if ($ActiveSmoke) {
        Write-Host ""
        Write-Host "Running explicitly requested active testnet round trip on the same account and journal."
        $active = Invoke-CopyTrader -Arguments @("testnet-active-smoke", "--coin", $Coin, "--size", $Size)
        Assert-Canary -Condition ($active.passed -eq $true) -Message "active testnet smoke did not pass"
        Assert-Canary -Condition ($active.safe_mode.enabled -ne $true) -Message "active testnet smoke ended in safe mode"
        Assert-Canary -Condition ($active.entry.status -eq "filled") -Message "active smoke entry did not fill"
        Assert-Canary -Condition ($active.exit.status -eq "filled") -Message "active smoke exit did not fill"
        Assert-Canary -Condition (@($active.after_reconcile.open_orders).Count -eq 0) -Message "active smoke left open orders"
        Assert-Canary -Condition (@($active.after_reconcile.positions.PSObject.Properties).Count -eq 0) -Message "active smoke did not finish flat"
        Assert-Canary -Condition ([decimal]$active.balance_delta -ne 0) -Message "active smoke did not observe a balance delta"

        $activeSettlement = Invoke-CopyTrader -Arguments @("settle-pending", "--mode", "testnet")
        Assert-Canary -Condition ([int]$activeSettlement.pending_after -eq 0) -Message "pending intents remain after active smoke"
        $activeRefresh = Invoke-CopyTrader -Arguments @("refresh-readiness-truth", "--mode", "testnet")
        Assert-Canary -Condition ($activeRefresh.passed -eq $true) -Message "post-active readiness truth refresh did not pass"
        $activeVerify = Invoke-CopyTrader -Arguments @("verify", "--mode", "testnet")
        Assert-Canary -Condition ($activeVerify.preflight.passed -eq $true) -Message "post-active verify preflight did not pass"
        Assert-Canary -Condition ($activeVerify.readiness.ready -eq $true) -Message "post-active verify readiness did not pass"
    }
}
finally {
    Restore-Env
    Pop-Location
}
