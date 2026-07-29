[CmdletBinding()]
param(
    [string[]]$Ports = @("9092", "9093", "9094"),
    [string]$EtcdEndpoints = "http://localhost:2379,http://localhost:2479,http://localhost:2579",
    [string]$ClusterId = "",
    [string]$RunDirectory = ".run/minimq-v4-failover",
    [string]$AdminToken = "demo-admin",
    [string]$Topic = "v4-failover-demo",
    [string]$Group = "v4-failover-demo-group",
    [string]$ComposeFile = "deploy/etcd/docker-compose.yml",
    [string]$ComposeProjectName = "",
    [ValidateRange(1, 3600)]
    [int]$ReadinessTimeoutSeconds = 90,
    [ValidateRange(1, 3600)]
    [int]$FailoverTimeoutSeconds = 45,
    [ValidateRange(1, 3600)]
    [int]$CommandTimeoutSeconds = 30,
    [ValidateRange(1, 3600)]
    [int]$StopTimeoutSeconds = 15,
    [switch]$SkipBuild,
    [switch]$KeepRunDirectory
)

$ErrorActionPreference = "Stop"
$commandSequence = 0
$ownershipMarkerFileName = ".minimq-run-directory"
$ownershipMarkerContent = "minimq-v4-owned-run-directory-v1"
$composeOwnershipMarkerFileName = ".minimq-compose-owned"
$composeOwnershipMarkerPrefix = "minimq-v4-owned-compose-project-v1:"

function ConvertTo-NativeArgument {
    param([AllowEmptyString()][string]$Value)

    if ($Value.Length -gt 0 -and $Value -notmatch '[\s"]') {
        return $Value
    }

    $quoted = New-Object System.Text.StringBuilder
    [void]$quoted.Append('"')
    $backslashCount = 0
    foreach ($character in $Value.ToCharArray()) {
        if ($character -eq '\') {
            $backslashCount++
            continue
        }
        if ($character -eq '"') {
            [void]$quoted.Append(('\' * (($backslashCount * 2) + 1)))
            [void]$quoted.Append('"')
            $backslashCount = 0
            continue
        }
        if ($backslashCount -gt 0) {
            [void]$quoted.Append(('\' * $backslashCount))
            $backslashCount = 0
        }
        [void]$quoted.Append($character)
    }
    if ($backslashCount -gt 0) {
        [void]$quoted.Append(('\' * ($backslashCount * 2)))
    }
    [void]$quoted.Append('"')
    return $quoted.ToString()
}

function Stop-NativeProcessTree {
    param([System.Diagnostics.Process]$Process)

    try {
        if ($null -eq $Process -or $Process.HasExited) {
            return
        }
    } catch {
        return
    }

    if ([Environment]::OSVersion.Platform -eq [PlatformID]::Win32NT) {
        $taskkillPath = Join-Path ([Environment]::GetFolderPath('System')) "taskkill.exe"
        $killer = Start-Process -FilePath $taskkillPath `
            -ArgumentList @("/PID", [string]$Process.Id, "/T", "/F") `
            -WindowStyle Hidden `
            -PassThru
        if (-not $killer.WaitForExit(5000)) {
            Stop-Process -Id $killer.Id -Force -ErrorAction SilentlyContinue
            throw "Timed out while terminating process tree rooted at PID $($Process.Id)."
        }
    } else {
        Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
    }
    if (-not $Process.WaitForExit(5000)) {
        throw "Process tree rooted at PID $($Process.Id) did not exit."
    }
}

function Read-TextFile {
    param([string]$Path)

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        return ""
    }
    return Get-Content -Raw -LiteralPath $Path
}

function Invoke-CapturedProcess {
    param(
        [string]$FilePath,
        [string[]]$ArgumentList,
        [string]$Name,
        [int]$TimeoutSeconds
    )

    $script:commandSequence++
    $safeName = $Name -replace '[^a-zA-Z0-9._-]', '-'
    $prefix = "{0:D2}-{1}" -f $script:commandSequence, $safeName
    $stdoutPath = Join-Path $logDirectory "$prefix.log"
    $stderrPath = Join-Path $logDirectory "$prefix.error.log"
    $arguments = ($ArgumentList | ForEach-Object { ConvertTo-NativeArgument ([string]$_) }) -join ' '
    $process = Start-Process -FilePath $FilePath `
        -ArgumentList $arguments `
        -WorkingDirectory $repoRoot `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath `
        -PassThru
    if (-not $process.WaitForExit($TimeoutSeconds * 1000)) {
        Stop-NativeProcessTree -Process $process
        $stdout = Read-TextFile $stdoutPath
        $stderr = Read-TextFile $stderrPath
        throw "$Name timed out after $TimeoutSeconds seconds.`nstdout:`n$stdout`nstderr:`n$stderr"
    }
    $process.Refresh()
    $stdout = Read-TextFile $stdoutPath
    $stderr = Read-TextFile $stderrPath
    if ($process.ExitCode -ne 0) {
        throw "$Name failed with exit code $($process.ExitCode).`nstdout:`n$stdout`nstderr:`n$stderr"
    }
    return ($stdout + $stderr)
}

function Invoke-DemoCommand {
    param(
        [string]$Name,
        [string[]]$Arguments,
        [int]$TimeoutSeconds = $CommandTimeoutSeconds
    )

    $output = Invoke-CapturedProcess `
        -FilePath "java" `
        -ArgumentList (@("-jar", $jar) + $Arguments) `
        -Name $Name `
        -TimeoutSeconds $TimeoutSeconds
    if (-not [string]::IsNullOrWhiteSpace($output)) {
        Write-Host $output.TrimEnd()
    }
    return $output
}

function Assert-OutputContains {
    param(
        [string]$Output,
        [string[]]$RequiredValues,
        [string]$Description
    )

    $missing = @($RequiredValues | Where-Object { -not $Output.Contains($_) })
    if ($missing.Count -gt 0) {
        throw "$Description missing expected evidence: $($missing -join ', ').`n$output"
    }
}

function Invoke-ConsumeUntilValue {
    param(
        [string]$ConsumerGroup,
        [string]$ExpectedValue,
        [string]$Name
    )

    $script:commandSequence++
    $prefix = "{0:D2}-{1}" -f $script:commandSequence, $Name
    $stdoutPath = Join-Path $logDirectory "$prefix.log"
    $stderrPath = Join-Path $logDirectory "$prefix.error.log"
    $arguments = @(
        "-jar", $jar,
        "consume",
        "--brokers", $bootstrapServers,
        "--topic", $Topic,
        "--group", $ConsumerGroup
    )
    $argumentLine = ($arguments | ForEach-Object { ConvertTo-NativeArgument ([string]$_) }) -join ' '
    $process = Start-Process -FilePath "java" `
        -ArgumentList $argumentLine `
        -WorkingDirectory $repoRoot `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath `
        -PassThru
    $deadline = [DateTime]::UtcNow.AddSeconds($CommandTimeoutSeconds)
    $matched = $false
    try {
        do {
            $stdout = Read-TextFile $stdoutPath
            if ($stdout.Contains("value=$ExpectedValue")) {
                $matched = $true
                break
            }
            if ($process.HasExited) {
                break
            }
            Start-Sleep -Milliseconds 250
        } while ([DateTime]::UtcNow -lt $deadline)
    } finally {
        if (-not $process.HasExited) {
            Stop-NativeProcessTree -Process $process
        }
    }

    $stdout = Read-TextFile $stdoutPath
    $stderr = Read-TextFile $stderrPath
    if (-not [string]::IsNullOrWhiteSpace($stdout)) {
        Write-Host $stdout.TrimEnd()
    }
    if (-not $matched) {
        throw "$Name did not consume value '$ExpectedValue' before the deadline.`nstdout:`n$stdout`nstderr:`n$stderr"
    }
    return $stdout
}

function Wait-ForLeader {
    param(
        [AllowNull()][Nullable[int]]$PreviousLeaderId,
        [DateTime]$Deadline,
        [string]$Phase,
        [bool]$RequireFullIsr = $false
    )

    $lastDescription = "no topic description captured"
    do {
        $remainingSeconds = ($Deadline - [DateTime]::UtcNow).TotalSeconds
        if ($remainingSeconds -le 0) {
            break
        }
        $probeTimeoutSeconds = [Math]::Max(
            1,
            [Math]::Min($CommandTimeoutSeconds, [Math]::Ceiling($remainingSeconds)))
        try {
            $lastDescription = Invoke-DemoCommand `
                -Name "$Phase-describe" `
                -Arguments @(
                    "describe-topic",
                    "--brokers", $bootstrapServers,
                    "--admin-token", $AdminToken,
                    "--topic", $Topic
                ) `
                -TimeoutSeconds $probeTimeoutSeconds
            $match = [regex]::Match(
                $lastDescription,
                'TopicPartitionInfo\[partitionId=0, leaderId=(-?\d+)')
            if ($match.Success) {
                $candidate = [int]$match.Groups[1].Value
                $changed = $null -eq $PreviousLeaderId -or
                    $candidate -ne [int]$PreviousLeaderId
                $ready = $lastDescription.Contains("readiness=READY") -and
                    $lastDescription.Contains("frozen=false")
                $fullIsr = -not $RequireFullIsr -or
                    $lastDescription.Contains("isrIds=V4BoundedList[total=3")
                if ($candidate -ge 0 -and $changed -and $ready -and $fullIsr) {
                    return $candidate
                }
            }
        } catch {
            $lastDescription = $_.Exception.Message
        }
        Start-Sleep -Milliseconds 500
    } while ([DateTime]::UtcNow -lt $Deadline)

    Write-Warning "$Phase Leader wait timed out. Last observed state:`n$lastDescription"
    throw "$Phase did not observe a ready Leader before the deadline."
}

function Get-BrokerEndpoint {
    param([int]$BrokerId)

    return "127.0.0.1:$($portValues[$BrokerId])"
}

$portValues = @()
foreach ($portEntry in $Ports) {
    foreach ($part in ($portEntry -split ',')) {
        if (-not [string]::IsNullOrWhiteSpace($part)) {
            $parsedPort = 0
            if (-not [int]::TryParse($part.Trim(), [ref]$parsedPort)) {
                throw "Invalid Broker port: $part."
            }
            $portValues += $parsedPort
        }
    }
}
if ($portValues.Count -ne 3) {
    throw "Ports must contain exactly three values."
}
if ([string]::IsNullOrWhiteSpace($AdminToken)) {
    throw "AdminToken must not be blank."
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$startScript = Join-Path $PSScriptRoot "start-brokers.ps1"
$stopScript = Join-Path $PSScriptRoot "stop-brokers.ps1"
$runDirectoryPath = if ([IO.Path]::IsPathRooted($RunDirectory)) {
    [IO.Path]::GetFullPath($RunDirectory)
} else {
    [IO.Path]::GetFullPath((Join-Path $repoRoot $RunDirectory))
}
$composeFilePath = if ([IO.Path]::IsPathRooted($ComposeFile)) {
    [IO.Path]::GetFullPath($ComposeFile)
} else {
    [IO.Path]::GetFullPath((Join-Path $repoRoot $ComposeFile))
}
$logDirectory = Join-Path $runDirectoryPath "logs"
$ownershipMarkerPath = Join-Path $runDirectoryPath $ownershipMarkerFileName
$composeOwnershipMarkerPath = Join-Path $runDirectoryPath $composeOwnershipMarkerFileName

if ([string]::IsNullOrWhiteSpace($ClusterId)) {
    $ClusterId = "minimq-v4-demo-$PID-$([DateTimeOffset]::UtcNow.ToUnixTimeSeconds())"
}
if ([string]::IsNullOrWhiteSpace($ComposeProjectName)) {
    $ComposeProjectName = "minimq-v4-demo-$PID"
}

$pom = [xml](Get-Content -Raw -LiteralPath (Join-Path $repoRoot "pom.xml"))
$version = [string]$pom.project.version
$jar = Join-Path $repoRoot "mq-demo\target\mq-demo-$version.jar"
$bootstrapServers = ($portValues | ForEach-Object { "127.0.0.1:$_" }) -join ','
$workflowFailure = $null
$expectedComposeOwnershipMarkerContent = $composeOwnershipMarkerPrefix + $ComposeProjectName

try {
    $startParameters = @{
        Ports = $portValues
        EtcdEndpoints = $EtcdEndpoints
        ClusterId = $ClusterId
        RunDirectory = $runDirectoryPath
        AdminToken = $AdminToken
        ComposeFile = $composeFilePath
        ComposeProjectName = $ComposeProjectName
        ReadinessTimeoutSeconds = $ReadinessTimeoutSeconds
        CommandTimeoutSeconds = [Math]::Max($CommandTimeoutSeconds, 120)
        RequireUnusedComposeProject = $true
        SkipBuild = $SkipBuild.IsPresent
    }
    & $startScript @startParameters

    Invoke-DemoCommand "create-topic" @(
        "create-topic",
        "--brokers", $bootstrapServers,
        "--admin-token", $AdminToken,
        "--name", $Topic,
        "--partitions", "1",
        "--replication-factor", "3"
    ) | Out-Null

    $statusEndpoint = Get-BrokerEndpoint 0
    Invoke-DemoCommand "ops-status-before" @(
        "ops-status", "--broker", $statusEndpoint, "--admin-token", $AdminToken
    ) | Out-Null
    Invoke-DemoCommand "produce-before" @(
        "produce", "--brokers", $bootstrapServers, "--topic", $Topic,
        "--key", "failover-key", "--message", "before-failover"
    ) | Out-Null
    Invoke-ConsumeUntilValue `
        -ConsumerGroup "$Group-before" `
        -ExpectedValue "before-failover" `
        -Name "consume-before" | Out-Null

    $leaderId = Wait-ForLeader `
        -PreviousLeaderId $null `
        -Deadline ([DateTime]::UtcNow.AddSeconds($FailoverTimeoutSeconds)) `
        -Phase "initial" `
        -RequireFullIsr $true
    Write-Host "Stopping current Leader broker-$leaderId."
    & $stopScript `
        -RunDirectory $runDirectoryPath `
        -BrokerId $leaderId `
        -StopTimeoutSeconds $StopTimeoutSeconds

    $newLeaderId = Wait-ForLeader `
        -PreviousLeaderId $leaderId `
        -Deadline ([DateTime]::UtcNow.AddSeconds($FailoverTimeoutSeconds)) `
        -Phase "failover"
    Write-Host "Failover complete: broker-$leaderId -> broker-$newLeaderId."

    $survivingIds = @(0, 1, 2 | Where-Object { $_ -ne $leaderId })
    $statusEndpoint = Get-BrokerEndpoint $survivingIds[0]
    $opsStatusAfter = Invoke-DemoCommand "ops-status-after" @(
        "ops-status", "--broker", $statusEndpoint, "--admin-token", $AdminToken
    )
    Assert-OutputContains `
        -Output $opsStatusAfter `
        -RequiredValues @(
            "Ops status targetBrokerId=$($survivingIds[0])",
            "maintenanceState=ACTIVE",
            "topicName=$Topic",
            "leaderId=$newLeaderId",
            "readiness=READY",
            "frozen=false"
        ) `
        -Description "post-failover ops status"
    Invoke-DemoCommand "produce-after" @(
        "produce", "--brokers", $bootstrapServers, "--topic", $Topic,
        "--key", "failover-key", "--message", "after-failover"
    ) | Out-Null
    Invoke-ConsumeUntilValue `
        -ConsumerGroup "$Group-after" `
        -ExpectedValue "after-failover" `
        -Name "consume-after" | Out-Null

    $auditOutputs = @()
    foreach ($brokerId in $survivingIds) {
        try {
            $auditOutput = Invoke-DemoCommand "failover-audit-broker-$brokerId" @(
                "failover-audit",
                "--broker", (Get-BrokerEndpoint $brokerId),
                "--admin-token", $AdminToken,
                "--limit", "100"
            )
            if ($auditOutput -match 'events=total=([1-9][0-9]*)') {
                $auditOutputs += $auditOutput
            }
        } catch {
            Write-Warning "Could not read failover audit from broker-$brokerId. $($_.Exception.Message)"
        }
    }
    if ($auditOutputs.Count -eq 0) {
        throw "No surviving Broker returned non-empty failover audit output."
    }
    $auditEvidence = $auditOutputs -join [Environment]::NewLine
    Assert-OutputContains `
        -Output $auditEvidence `
        -RequiredValues @(
            "eventType=LEADER_CHANGED",
            "topicName=$Topic",
            "oldValue=$leaderId",
            "newValue=$newLeaderId"
        ) `
        -Description "failover audit"

    Write-Host "V4 failover demo proved produce and fetch recovery."
} catch {
    $workflowFailure = $_
} finally {
    $cleanupFailures = @()
    try {
        & $stopScript `
            -RunDirectory $runDirectoryPath `
            -StopTimeoutSeconds $StopTimeoutSeconds
    } catch {
        $cleanupFailures += $_
        Write-Warning "Broker cleanup failed. $($_.Exception.Message)"
    }

    $ownsComposeProject =
        (Test-Path -LiteralPath $ownershipMarkerPath -PathType Leaf) -and
        (Get-Content -Raw -LiteralPath $ownershipMarkerPath) -eq $ownershipMarkerContent -and
        (Test-Path -LiteralPath $composeOwnershipMarkerPath -PathType Leaf) -and
        (Get-Content -Raw -LiteralPath $composeOwnershipMarkerPath) -eq
            $expectedComposeOwnershipMarkerContent
    if ($ownsComposeProject) {
        try {
            Invoke-CapturedProcess `
                -FilePath "docker" `
                -ArgumentList @(
                    "compose", "--project-name", $ComposeProjectName,
                    "-f", $composeFilePath, "down", "--volumes"
                ) `
                -Name "etcd-compose-down" `
                -TimeoutSeconds ([Math]::Max($CommandTimeoutSeconds, 60)) | Out-Null
            Remove-Item -LiteralPath $composeOwnershipMarkerPath -Force
        } catch {
            $cleanupFailures += $_
            Write-Warning "etcd compose cleanup failed. $($_.Exception.Message)"
        }
    }

    $composeCleanupPending = Test-Path -LiteralPath $composeOwnershipMarkerPath -PathType Leaf
    if (-not $KeepRunDirectory -and -not $composeCleanupPending) {
        try {
            & $stopScript `
                -RunDirectory $runDirectoryPath `
                -StopTimeoutSeconds $StopTimeoutSeconds `
                -CleanupRunDirectory
        } catch {
            $cleanupFailures += $_
            Write-Warning "Run-directory cleanup failed. $($_.Exception.Message)"
        }
    } elseif (-not $KeepRunDirectory -and $composeCleanupPending) {
        Write-Warning (
            "Preserving run directory because Compose cleanup did not remove ownership marker " +
            "$composeOwnershipMarkerPath.")
    }

    if ($null -eq $workflowFailure -and $cleanupFailures.Count -gt 0) {
        $workflowFailure = $cleanupFailures[0]
    }
}

if ($null -ne $workflowFailure) {
    throw $workflowFailure
}
