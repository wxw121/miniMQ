[CmdletBinding()]
param(
    [string]$RunDirectory = ".run/minimq-v4",
    [int[]]$BrokerId = @(),
    [ValidateRange(1, 3600)]
    [int]$StopTimeoutSeconds = 15,
    [switch]$CleanupRunDirectory
)

$ErrorActionPreference = "Stop"
$manifestFileName = "broker-processes.json"
$ownershipMarkerFileName = ".minimq-run-directory"
$ownershipMarkerContent = "minimq-v4-owned-run-directory-v1"

function Assert-RunDirectoryOwnership {
    if (-not (Test-Path -LiteralPath $ownershipMarkerPath -PathType Leaf) -or
        (Get-Content -Raw -LiteralPath $ownershipMarkerPath) -ne $ownershipMarkerContent) {
        throw "Refusing to process Broker manifest without a valid ownership marker: $runDirectoryPath."
    }
}

function Assert-BrokerManifest {
    param($Manifest)

    try {
        $schemaVersion = [int]$Manifest.schemaVersion
    } catch {
        throw "Broker PID manifest has an invalid schemaVersion: $manifestPath."
    }
    if ($schemaVersion -ne 1) {
        throw "Broker PID manifest uses unsupported schemaVersion ${schemaVersion}: $manifestPath."
    }

    try {
        $manifestRunDirectory = [IO.Path]::GetFullPath([string]$Manifest.runDirectory)
    } catch {
        throw "Broker PID manifest has an invalid runDirectory: $manifestPath."
    }
    $directorySeparators = [char[]]@(
        [IO.Path]::DirectorySeparatorChar,
        [IO.Path]::AltDirectorySeparatorChar)
    $expectedDirectory = $runDirectoryPath.TrimEnd($directorySeparators)
    $actualDirectory = $manifestRunDirectory.TrimEnd($directorySeparators)
    if (-not [string]::Equals(
            $expectedDirectory,
            $actualDirectory,
            [StringComparison]::OrdinalIgnoreCase)) {
        throw "Broker PID manifest runDirectory does not match $runDirectoryPath."
    }
    if ($null -eq $Manifest.brokers) {
        throw "Broker PID manifest does not contain a brokers array: $manifestPath."
    }

    $seenBrokerIds = New-Object 'System.Collections.Generic.HashSet[int]'
    foreach ($entry in @($Manifest.brokers)) {
        try {
            $pidValue = [int]$entry.pid
            $brokerIdValue = [int]$entry.brokerId
            $portValue = [int]$entry.port
            [void][DateTimeOffset]::Parse([string]$entry.processStartTimeUtc)
        } catch {
            throw "Broker PID manifest contains an invalid Broker entry: $manifestPath."
        }
        if ($pidValue -le 0 -or $brokerIdValue -lt 0 -or
            $portValue -lt 1 -or $portValue -gt 65535 -or
            -not $seenBrokerIds.Add($brokerIdValue)) {
            throw "Broker PID manifest contains an invalid or duplicate Broker entry: $manifestPath."
        }
    }
}

function Get-ManifestProcessState {
    param($Entry)

    try {
        $process = Get-Process -Id ([int]$Entry.pid) -ErrorAction Stop
    } catch {
        return [pscustomobject]@{ status = "exited"; process = $null }
    }

    try {
        $expectedStart = [DateTimeOffset]::Parse([string]$Entry.processStartTimeUtc).UtcDateTime
        if ($process.StartTime.ToUniversalTime().Ticks -ne $expectedStart.Ticks) {
            return [pscustomobject]@{ status = "identity-mismatch"; process = $process }
        }
    } catch {
        return [pscustomobject]@{ status = "identity-mismatch"; process = $process }
    }
    return [pscustomobject]@{ status = "owned"; process = $process }
}

function Write-BrokerManifest {
    param(
        $OriginalManifest,
        [object[]]$Entries
    )

    $updated = [ordered]@{
        schemaVersion = $OriginalManifest.schemaVersion
        createdAtUtc = $OriginalManifest.createdAtUtc
        runDirectory = $OriginalManifest.runDirectory
        composeFile = $OriginalManifest.composeFile
        composeProjectName = $OriginalManifest.composeProjectName
        brokers = @($Entries)
    }
    $temporaryPath = "$manifestPath.tmp"
    $utf8 = New-Object System.Text.UTF8Encoding($false)
    [IO.File]::WriteAllText(
        $temporaryPath,
        ($updated | ConvertTo-Json -Depth 6),
        $utf8)
    Move-Item -LiteralPath $temporaryPath -Destination $manifestPath -Force
}

function Remove-RunDirectory {
    $normalizedTarget = $runDirectoryPath.TrimEnd([IO.Path]::DirectorySeparatorChar)
    $normalizedRoot = ([IO.Path]::GetPathRoot($runDirectoryPath)).TrimEnd([IO.Path]::DirectorySeparatorChar)
    $normalizedRepo = $repoRoot.TrimEnd([IO.Path]::DirectorySeparatorChar)
    if ([string]::IsNullOrWhiteSpace($normalizedTarget) -or
        $normalizedTarget -eq $normalizedRoot -or
        $normalizedTarget -eq $normalizedRepo) {
        throw "Refusing to clean unsafe run directory: $runDirectoryPath."
    }
    if (-not (Test-Path -LiteralPath $runDirectoryPath)) {
        return
    }
    if (-not (Test-Path -LiteralPath $ownershipMarkerPath -PathType Leaf) -or
        (Get-Content -Raw -LiteralPath $ownershipMarkerPath) -ne $ownershipMarkerContent) {
        throw "Refusing to clean RunDirectory without a valid ownership marker: $runDirectoryPath."
    }
    Remove-Item -LiteralPath $runDirectoryPath -Recurse -Force
    Write-Host "Removed run directory $runDirectoryPath."
}

if ([string]::IsNullOrWhiteSpace($RunDirectory)) {
    throw "RunDirectory must not be blank."
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$runDirectoryPath = if ([IO.Path]::IsPathRooted($RunDirectory)) {
    [IO.Path]::GetFullPath($RunDirectory)
} else {
    [IO.Path]::GetFullPath((Join-Path $repoRoot $RunDirectory))
}
$manifestPath = Join-Path $runDirectoryPath $manifestFileName
$ownershipMarkerPath = Join-Path $runDirectoryPath $ownershipMarkerFileName

if (-not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) {
    Write-Host "No Broker PID manifest found at $manifestPath. Nothing to stop."
    if ($CleanupRunDirectory) {
        Remove-RunDirectory
    }
    return
}

Assert-RunDirectoryOwnership

try {
    $manifest = Get-Content -Raw -LiteralPath $manifestPath | ConvertFrom-Json
} catch {
    throw "Cannot read Broker PID manifest $manifestPath. $($_.Exception.Message)"
}
Assert-BrokerManifest $manifest
$entries = @($manifest.brokers)
$requestedIds = @($BrokerId | Select-Object -Unique)
$stopAll = $requestedIds.Count -eq 0
$manifestIds = @($entries | ForEach-Object { [int]$_.brokerId })
$unknownIds = @($requestedIds | Where-Object { $manifestIds -notcontains $_ })
if ($unknownIds.Count -gt 0) {
    throw "Broker ID(s) $($unknownIds -join ', ') are not present in the Broker manifest."
}
$untouchedEntries = @()
$signaledEntries = @()

foreach ($entry in $entries) {
    $selected = $stopAll -or $requestedIds -contains ([int]$entry.brokerId)
    if (-not $selected) {
        $untouchedEntries += $entry
        continue
    }

    $state = Get-ManifestProcessState $entry
    switch ($state.status) {
        "owned" {
            Write-Host "Stopping broker-$($entry.brokerId) PID $($entry.pid)..."
            try {
                Stop-Process -Id $state.process.Id -Force -ErrorAction Stop
            } catch {
                if ((Get-ManifestProcessState $entry).status -eq "owned") {
                    throw
                }
                Write-Host "broker-$($entry.brokerId) exited before it could be signaled."
            }
            $signaledEntries += $entry
        }
        "identity-mismatch" {
            Write-Warning "PID $($entry.pid) no longer matches broker-$($entry.brokerId) start time; it was not stopped."
        }
        default {
            Write-Host "broker-$($entry.brokerId) PID $($entry.pid) has already exited."
        }
    }
}

$deadline = [DateTime]::UtcNow.AddSeconds($StopTimeoutSeconds)
do {
    $survivingEntries = @($signaledEntries | Where-Object {
            (Get-ManifestProcessState $_).status -eq "owned"
        })
    if ($survivingEntries.Count -eq 0) {
        break
    }
    Start-Sleep -Milliseconds 200
} while ([DateTime]::UtcNow -lt $deadline)

$remainingEntries = @($untouchedEntries) + @($survivingEntries)
if ($remainingEntries.Count -eq 0) {
    Remove-Item -LiteralPath $manifestPath -Force
    Write-Host "All selected owned Broker processes exited; removed $manifestPath."
} else {
    Write-BrokerManifest $manifest $remainingEntries
    if ($survivingEntries.Count -gt 0) {
        Write-Warning "Owned Broker processes still running after $StopTimeoutSeconds seconds: $($survivingEntries.pid -join ', ')."
    }
    if ($untouchedEntries.Count -gt 0) {
        Write-Host "Retained manifest entries for broker IDs: $($untouchedEntries.brokerId -join ', ')."
    }
}

if ($CleanupRunDirectory) {
    if ($remainingEntries.Count -gt 0) {
        throw "Cannot clean $runDirectoryPath while Broker manifest entries remain."
    }
    Remove-RunDirectory
}

if ($survivingEntries.Count -gt 0) {
    throw "Timed out waiting for owned Broker processes to exit."
}
