[CmdletBinding()]
param(
    [switch]$DryRun,
    [switch]$ConfirmProduction,
    [string]$Python = "python",
    [string]$KnownHostsPath,
    [ValidatePattern('^[a-zA-Z0-9.-]+$')][string]$ServerHost = "31.131.31.117",
    [ValidatePattern('^[a-zA-Z0-9_-]+$')][string]$ServerUser = "deploy",
    [ValidateRange(1, 65535)][int]$ServerPort = 22,
    [string]$ArtifactDirectory,
    [string]$BackupReceiptPath
)
$ErrorActionPreference = "Stop"
$codeDir = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$builder = Join-Path $codeDir "ops/release_artifact.py"
# Deliberately use ssh-agent / the operator's SSH config. Never inspect key files.
$sshClient = "ssh"
$scpClient = "scp"
if ($env:SystemRoot) {
    $windowsOpenSsh = Join-Path $env:SystemRoot "System32\OpenSSH"
    $windowsSsh = Join-Path $windowsOpenSsh "ssh.exe"
    $windowsScp = Join-Path $windowsOpenSsh "scp.exe"
    if ((Test-Path -LiteralPath $windowsSsh -PathType Leaf) -and (Test-Path -LiteralPath $windowsScp -PathType Leaf)) {
        $sshClient = $windowsSsh
        $scpClient = $windowsScp
    }
}
if ($DryRun) {
    $dryZip = Join-Path ([System.IO.Path]::GetTempPath()) ("mcf-source-" + [guid]::NewGuid().ToString("N") + ".zip")
    & $Python $builder build --source $codeDir --archive $dryZip
    if ($LASTEXITCODE -ne 0) { throw "Safe archive build failed" }
    & $Python $builder verify --archive $dryZip
    if ($LASTEXITCODE -ne 0) { throw "Archive validation failed" }
    Write-Host "Source-only dry run; no keys read, no network or deployment. $dryZip"
    exit 0
}
if (-not $ConfirmProduction) { throw "New explicit approval required: -ConfirmProduction" }
if (-not $KnownHostsPath -or -not (Test-Path -LiteralPath $KnownHostsPath -PathType Leaf)) {
    throw "Provide a preverified pinned known_hosts file; do not trust ssh-keyscan on this connection."
}
if (-not $ArtifactDirectory) { throw "Provide the successful CI artifact directory; local rebuilds cannot deploy." }
$zip = Join-Path $ArtifactDirectory "release.zip"
$images = Join-Path $ArtifactDirectory "images.tar"
$evidencePath = Join-Path $ArtifactDirectory "release-evidence.json"
$evidence = Get-Content -LiteralPath $evidencePath -Raw | ConvertFrom-Json
if ($evidence.validation -ne "passed" -or $evidence.release_id -notmatch '^[a-f0-9]{40}$') { throw "Invalid CI evidence" }
$releaseId = $evidence.release_id
foreach ($item in @(@($zip, $evidence.source_sha256), @($images, $evidence.images_sha256))) {
    if ((Get-FileHash -LiteralPath $item[0] -Algorithm SHA256).Hash.ToLower() -ne $item[1]) { throw "Artifact checksum mismatch" }
}
$transportArgs = @("-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=yes", "-o", "UserKnownHostsFile=$KnownHostsPath", "-o", "GlobalKnownHostsFile=none")
$sshArgs = @("-p", "$ServerPort") + $transportArgs
$scpArgs = @("-P", "$ServerPort") + $transportArgs
$destination = "${ServerUser}@${ServerHost}"
# One-time cleanup of the exact orphan recorded by failed production run
# 38720060373. Avoid wildcard/age cleanup, which could delete an active rollout.
$knownFailedStagingCleanup = "rm -rf -- '/tmp/mcf-release.z5rIi8c2'"
& $sshClient @sshArgs $destination $knownFailedStagingCleanup
if ($LASTEXITCODE -ne 0) { throw "Known failed staging cleanup failed" }
$remote = "/tmp/mcf-release.$([guid]::NewGuid().ToString('N'))"
$remoteCleanup = "rm -rf -- '$remote'"
$remoteCreate = "mkdir -m 700 -- '$remote'"
& $sshClient @sshArgs $destination $remoteCreate
if ($LASTEXITCODE -ne 0) {
    & $sshClient @sshArgs $destination $remoteCleanup | Out-Null
    throw "Verified SSH staging creation failed"
}
$deploymentError = $null
$cleanupExitCode = 0
try {
& $scpClient @scpArgs $zip "${destination}:${remote}/release.zip"
if ($LASTEXITCODE -ne 0) {
    & $sshClient @sshArgs $destination $remoteCleanup | Out-Null
    throw "Source upload failed"
}
& $scpClient @scpArgs $images "${destination}:${remote}/images.tar"
if ($LASTEXITCODE -ne 0) {
    & $sshClient @sshArgs $destination $remoteCleanup | Out-Null
    throw "Image upload failed"
}
& $scpClient @scpArgs $evidencePath "${destination}:${remote}/release-evidence.json"
if ($LASTEXITCODE -ne 0) {
    & $sshClient @sshArgs $destination $remoteCleanup | Out-Null
    throw "Evidence upload failed"
}
& $scpClient @scpArgs $builder "${destination}:${remote}/release_artifact.py"
if ($LASTEXITCODE -ne 0) {
    & $sshClient @sshArgs $destination $remoteCleanup | Out-Null
    throw "Validator upload failed"
}
$backupRemoteArgument = ""
if ($BackupReceiptPath) {
    if (-not (Test-Path -LiteralPath $BackupReceiptPath -PathType Leaf)) { throw "Backup receipt is missing" }
    $backupReceipt = Get-Content -LiteralPath $BackupReceiptPath -Raw | ConvertFrom-Json
    if ($backupReceipt.off_host_verified -ne $true -or $backupReceipt.backup_filename -notmatch '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$') {
        throw "Invalid off-host backup receipt"
    }
    $backupCiphertext = Join-Path (Split-Path -Parent $BackupReceiptPath) $backupReceipt.backup_filename
    if (-not (Test-Path -LiteralPath $backupCiphertext -PathType Leaf)) { throw "Encrypted backup is missing" }
    $backupHash = (Get-FileHash -LiteralPath $backupCiphertext -Algorithm SHA256).Hash.ToLower()
    if ($backupHash -ne $backupReceipt.server_backup_sha256 -or $backupHash -ne $backupReceipt.off_host_sha256) {
        throw "Off-host backup checksum mismatch"
    }
    & $scpClient @scpArgs $backupCiphertext "${destination}:${remote}/$($backupReceipt.backup_filename)"
    if ($LASTEXITCODE -ne 0) {
        & $sshClient @sshArgs $destination $remoteCleanup | Out-Null
        throw "Encrypted backup upload failed"
    }
    & $scpClient @scpArgs $BackupReceiptPath "${destination}:${remote}/backup-receipt.json"
    if ($LASTEXITCODE -ne 0) {
        & $sshClient @sshArgs $destination $remoteCleanup | Out-Null
        throw "Backup receipt upload failed"
    }
    $backupRemoteArgument = " --backup-receipt '$remote/backup-receipt.json'"
}
$sourceSha = $evidence.source_sha256
$remoteCommand = "set -eu; python3 '$remote/release_artifact.py' install --archive '$remote/release.zip' --sha256 '$sourceSha' --target '$remote/stage'; bash '$remote/stage/deploy_v0_on_vps.sh' --confirm-production '$remote' --source-sha256 '$sourceSha'$backupRemoteArgument"
$previousErrorActionPreference = $ErrorActionPreference
try {
    # OpenSSH writes normal remote progress to stderr. Judge the rollout by its
    # native exit code instead of converting progress output into an exception.
    $ErrorActionPreference = "Continue"
    & $sshClient @sshArgs $destination $remoteCommand
    $rolloutExitCode = $LASTEXITCODE
} finally {
    $ErrorActionPreference = $previousErrorActionPreference
}
if ($rolloutExitCode -ne 0) { throw "Rollout failed: serving/writers are stopped; inspect approved incident runbook. No automatic database downgrade." }
} catch {
    $deploymentError = $_
} finally {
    & $sshClient @sshArgs $destination $remoteCleanup | Out-Null
    $cleanupExitCode = $LASTEXITCODE
}
if ($null -ne $deploymentError -and $cleanupExitCode -ne 0) {
    throw "Deployment failed: $($deploymentError.Exception.Message); remote staging cleanup also failed"
}
if ($null -ne $deploymentError) { throw $deploymentError }
if ($cleanupExitCode -ne 0) { throw "Remote staging cleanup failed" }
Write-Host "Validated release ready. Record real deployment evidence and version under release policy."
