[CmdletBinding()]
param(
    [string[]]$Ports = @("9092", "9093", "9094"),
    [string]$EtcdEndpoints = "http://localhost:2379,http://localhost:2479,http://localhost:2579",
    [string]$ClusterId = "minimq",
    [string]$RunDirectory = ".run/minimq-v4",
    [string]$AdminToken = "demo-admin",
    [string]$ComposeFile = "deploy/etcd/docker-compose.yml",
    [string]$ComposeProjectName = "minimq-local",
    [ValidateRange(1, 3600)]
    [int]$ReadinessTimeoutSeconds = 60,
    [ValidateRange(1, 3600)]
    [int]$CommandTimeoutSeconds = 120,
    [switch]$RequireUnusedComposeProject,
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"
$manifestFileName = "broker-processes.json"
$ownershipMarkerFileName = ".minimq-run-directory"
$ownershipMarkerContent = "minimq-v4-owned-run-directory-v1"
$composeOwnershipMarkerFileName = ".minimq-compose-owned"
$composeOwnershipMarkerPrefix = "minimq-v4-owned-compose-project-v1:"
$ownedBrokers = @()

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

function Start-BoundedProcess {
    param(
        [string]$FilePath,
        [string[]]$ArgumentList,
        [string]$WorkingDirectory,
        [string]$StandardOutputPath,
        [string]$StandardErrorPath,
        [int]$TimeoutSeconds,
        [string]$Description
    )

    $arguments = ($ArgumentList | ForEach-Object { ConvertTo-NativeArgument ([string]$_) }) -join ' '
    $process = Start-Process -FilePath $FilePath `
        -ArgumentList $arguments `
        -WorkingDirectory $WorkingDirectory `
        -RedirectStandardOutput $StandardOutputPath `
        -RedirectStandardError $StandardErrorPath `
        -PassThru
    if (-not $process.WaitForExit($TimeoutSeconds * 1000)) {
        Stop-NativeProcessTree -Process $process
        throw "$Description did not finish within $TimeoutSeconds seconds. See $StandardErrorPath."
    }
    $process.Refresh()
    if ($process.ExitCode -ne 0) {
        $detail = if (Test-Path -LiteralPath $StandardErrorPath) {
            (Get-Content -LiteralPath $StandardErrorPath -Tail 20) -join [Environment]::NewLine
        } else {
            "no error output"
        }
        throw "$Description failed with exit code $($process.ExitCode). $detail"
    }
}

function Test-TcpEndpoint {
    param(
        [string]$HostName,
        [int]$Port,
        [int]$TimeoutMilliseconds = 250
    )

    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $pending = $client.BeginConnect($HostName, $Port, $null, $null)
        if (-not $pending.AsyncWaitHandle.WaitOne($TimeoutMilliseconds, $false)) {
            return $false
        }
        $client.EndConnect($pending)
        return $true
    } catch {
        return $false
    } finally {
        $client.Close()
    }
}

function Get-ManifestOwnedProcess {
    param($Entry)

    try {
        $process = Get-Process -Id ([int]$Entry.pid) -ErrorAction Stop
        $expectedStart = [DateTimeOffset]::Parse([string]$Entry.processStartTimeUtc).UtcDateTime
        if ($process.StartTime.ToUniversalTime().Ticks -ne $expectedStart.Ticks) {
            return $null
        }
        return $process
    } catch {
        return $null
    }
}

function Write-BrokerManifest {
    param([object[]]$Entries)

    $manifest = [ordered]@{
        schemaVersion = 1
        createdAtUtc = [DateTimeOffset]::UtcNow.ToString("o")
        runDirectory = $runDirectoryPath
        composeFile = $composeFilePath
        composeProjectName = $ComposeProjectName
        brokers = @($Entries)
    }
    $temporaryPath = "$manifestPath.tmp"
    $utf8 = New-Object System.Text.UTF8Encoding($false)
    [IO.File]::WriteAllText(
        $temporaryPath,
        ($manifest | ConvertTo-Json -Depth 6),
        $utf8)
    Move-Item -LiteralPath $temporaryPath -Destination $manifestPath -Force
}

function Stop-StartedBrokers {
    $deadline = [DateTime]::UtcNow.AddSeconds(10)
    foreach ($entry in @($ownedBrokers)) {
        $process = Get-ManifestOwnedProcess $entry
        if ($null -ne $process) {
            Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
        }
    }

    do {
        $survivors = @($ownedBrokers | Where-Object {
                $null -ne (Get-ManifestOwnedProcess $_)
            })
        if ($survivors.Count -eq 0) {
            break
        }
        Start-Sleep -Milliseconds 200
    } while ([DateTime]::UtcNow -lt $deadline)

    if ($survivors.Count -eq 0) {
        Remove-Item -LiteralPath $manifestPath -Force -ErrorAction SilentlyContinue
    } else {
        Write-BrokerManifest $survivors
        Write-Warning "Startup cleanup could not stop owned Broker PIDs: $($survivors.pid -join ', ')."
    }
}

function Wait-ForEtcd {
    param(
        [uri[]]$Endpoints,
        [DateTime]$Deadline
    )

    do {
        $unavailable = @($Endpoints | Where-Object {
                -not (Test-TcpEndpoint $_.Host ([int]$_.Port))
            })
        if ($unavailable.Count -eq 0) {
            return
        }
        Start-Sleep -Milliseconds 250
    } while ([DateTime]::UtcNow -lt $Deadline)

    throw "etcd readiness timed out. Unavailable endpoints: $($unavailable.AbsoluteUri -join ', ')."
}

function Wait-ForBroker {
    param(
        $Entry,
        [DateTime]$Deadline
    )

    do {
        if ($null -eq (Get-ManifestOwnedProcess $Entry)) {
            $logTail = if (Test-Path -LiteralPath $Entry.logPath) {
                (Get-Content -LiteralPath $Entry.logPath -Tail 20) -join [Environment]::NewLine
            } else {
                "no Broker log output"
            }
            throw "broker-$($Entry.brokerId) exited before readiness. $logTail"
        }
        if (Test-TcpEndpoint "127.0.0.1" ([int]$Entry.port)) {
            return
        }
        Start-Sleep -Milliseconds 250
    } while ([DateTime]::UtcNow -lt $Deadline)

    throw "broker-$($Entry.brokerId) readiness timed out on port $($Entry.port). Log: $($Entry.logPath)."
}

function Get-BrokerProtocolReadiness {
    param(
        $Entry,
        [int]$TimeoutSeconds
    )

    $stdoutPath = Join-Path $logDirectory "readiness-broker-$($Entry.brokerId).log"
    $stderrPath = Join-Path $logDirectory "readiness-broker-$($Entry.brokerId).error.log"
    try {
        Start-BoundedProcess `
            -FilePath "java" `
            -ArgumentList @(
                "-jar", $jar,
                "ops-status",
                "--broker", "127.0.0.1:$($Entry.port)",
                "--admin-token", $AdminToken
            ) `
            -WorkingDirectory $repoRoot `
            -StandardOutputPath $stdoutPath `
            -StandardErrorPath $stderrPath `
            -TimeoutSeconds $TimeoutSeconds `
            -Description "broker-$($Entry.brokerId) protocol readiness probe"
        $output = Get-Content -Raw -LiteralPath $stdoutPath
        $onlineBrokerCount = [regex]::Matches(
            $output,
            'OpsBrokerInfo\[[^\]]*state=ONLINE[^\]]*\]').Count
        $ready = $output.Contains("Ops status targetBrokerId=$($Entry.brokerId)") -and
            $output.Contains("maintenanceState=ACTIVE") -and
            $output.Contains("brokers=total=3 truncated=false") -and
            $onlineBrokerCount -eq 3
        return [pscustomobject]@{
            brokerId = $Entry.brokerId
            ready = $ready
            detail = $output.Trim()
        }
    } catch {
        return [pscustomobject]@{
            brokerId = $Entry.brokerId
            ready = $false
            detail = $_.Exception.Message
        }
    }
}

function Wait-ForClusterReadiness {
    param([DateTime]$Deadline)

    $states = @()
    do {
        $states = @()
        foreach ($entry in @($ownedBrokers)) {
            $remainingSeconds = [int][Math]::Ceiling(($Deadline - [DateTime]::UtcNow).TotalSeconds)
            if ($remainingSeconds -le 0) {
                break
            }
            $probeTimeout = [Math]::Max(1, [Math]::Min(3, $remainingSeconds))
            $states += Get-BrokerProtocolReadiness $entry $probeTimeout
        }
        if ($states.Count -eq 3 -and
            @($states | Where-Object { -not $_.ready }).Count -eq 0) {
            return
        }
        Start-Sleep -Milliseconds 250
    } while ([DateTime]::UtcNow -lt $Deadline)

    $stateSummary = @($states | ForEach-Object {
            "broker-$($_.brokerId): $($_.detail -replace '[\r\n]+', ' ')"
        }) -join [Environment]::NewLine
    throw "Broker cluster readiness timed out. Last observed state:`n$stateSummary"
}

function Test-ComposeProjectHasResources {
    $checks = @(
        [pscustomobject]@{
            Name = "containers"
            Arguments = @(
                "ps", "--all", "--quiet", "--filter",
                "label=com.docker.compose.project=$ComposeProjectName")
        },
        [pscustomobject]@{
            Name = "volumes"
            Arguments = @(
                "volume", "ls", "--quiet", "--filter",
                "label=com.docker.compose.project=$ComposeProjectName")
        },
        [pscustomobject]@{
            Name = "networks"
            Arguments = @(
                "network", "ls", "--quiet", "--filter",
                "label=com.docker.compose.project=$ComposeProjectName")
        }
    )

    foreach ($check in $checks) {
        $stdoutPath = Join-Path $logDirectory "compose-preflight-$($check.Name).log"
        $stderrPath = Join-Path $logDirectory "compose-preflight-$($check.Name).error.log"
        Start-BoundedProcess `
            -FilePath "docker" `
            -ArgumentList $check.Arguments `
            -WorkingDirectory $repoRoot `
            -StandardOutputPath $stdoutPath `
            -StandardErrorPath $stderrPath `
            -TimeoutSeconds $CommandTimeoutSeconds `
            -Description "compose project $($check.Name) preflight"
        if ((Get-Content -Raw -LiteralPath $stdoutPath).Trim().Length -gt 0) {
            return $true
        }
    }
    return $false
}

function Remove-OwnedComposeProject {
    if (-not $RequireUnusedComposeProject -or
        -not (Test-Path -LiteralPath $composeOwnershipMarkerPath -PathType Leaf) -or
        (Get-Content -Raw -LiteralPath $composeOwnershipMarkerPath) -ne
            $expectedComposeOwnershipMarkerContent) {
        return
    }

    try {
        Start-BoundedProcess `
            -FilePath "docker" `
            -ArgumentList @(
                "compose", "--project-name", $ComposeProjectName,
                "-f", $composeFilePath, "down", "--volumes") `
            -WorkingDirectory $repoRoot `
            -StandardOutputPath (Join-Path $logDirectory "etcd-compose-cleanup.log") `
            -StandardErrorPath (Join-Path $logDirectory "etcd-compose-cleanup.error.log") `
            -TimeoutSeconds $CommandTimeoutSeconds `
            -Description "owned etcd compose cleanup"
        Remove-Item -LiteralPath $composeOwnershipMarkerPath -Force
    } catch {
        Write-Warning "Owned compose cleanup failed. $($_.Exception.Message)"
    }
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
    throw "Ports must contain exactly three values (one per Broker)."
}
if (($portValues | Select-Object -Unique).Count -ne 3 -or ($portValues | Where-Object {
            $_ -lt 1 -or $_ -gt 65535
        }).Count -ne 0) {
    throw "Ports must be three unique values between 1 and 65535."
}
if ([string]::IsNullOrWhiteSpace($AdminToken)) {
    throw "AdminToken must not be blank."
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
$composeFilePath = if ([IO.Path]::IsPathRooted($ComposeFile)) {
    [IO.Path]::GetFullPath($ComposeFile)
} else {
    [IO.Path]::GetFullPath((Join-Path $repoRoot $ComposeFile))
}
$manifestPath = Join-Path $runDirectoryPath $manifestFileName
$ownershipMarkerPath = Join-Path $runDirectoryPath $ownershipMarkerFileName
$composeOwnershipMarkerPath = Join-Path $runDirectoryPath $composeOwnershipMarkerFileName
$expectedComposeOwnershipMarkerContent = $composeOwnershipMarkerPrefix + $ComposeProjectName
$logDirectory = Join-Path $runDirectoryPath "logs"
$dataDirectory = Join-Path $runDirectoryPath "data"

if (-not (Test-Path -LiteralPath $composeFilePath -PathType Leaf)) {
    throw "etcd compose file not found at $composeFilePath."
}
if (Test-Path -LiteralPath $runDirectoryPath) {
    if (-not (Test-Path -LiteralPath $runDirectoryPath -PathType Container)) {
        throw "RunDirectory must be a directory: $runDirectoryPath."
    }
    if (Test-Path -LiteralPath $ownershipMarkerPath -PathType Leaf) {
        if ((Get-Content -Raw -LiteralPath $ownershipMarkerPath) -ne $ownershipMarkerContent) {
            throw "RunDirectory has an invalid ownership marker: $runDirectoryPath."
        }
    } elseif (@(Get-ChildItem -Force -LiteralPath $runDirectoryPath).Count -gt 0) {
        throw "Refusing to use non-empty RunDirectory without an ownership marker: $runDirectoryPath."
    }
} else {
    New-Item -ItemType Directory -Path $runDirectoryPath | Out-Null
}
if (-not (Test-Path -LiteralPath $ownershipMarkerPath -PathType Leaf)) {
    $utf8 = New-Object System.Text.UTF8Encoding($false)
    [IO.File]::WriteAllText($ownershipMarkerPath, $ownershipMarkerContent, $utf8)
}
New-Item -ItemType Directory -Force -Path $logDirectory, $dataDirectory | Out-Null
if (Test-Path -LiteralPath $manifestPath) {
    throw "Broker manifest already exists at $manifestPath. Run stop-brokers.ps1 first."
}

$pom = [xml](Get-Content -Raw -LiteralPath (Join-Path $repoRoot "pom.xml"))
$version = [string]$pom.project.version
$jar = Join-Path $repoRoot "mq-demo\target\mq-demo-$version.jar"

try {
    if (-not $SkipBuild) {
        $mavenStdout = Join-Path $logDirectory "maven-build.log"
        $mavenStderr = Join-Path $logDirectory "maven-build.error.log"
        Start-BoundedProcess `
            -FilePath "mvn" `
            -ArgumentList @(
                "-q", "-gs", ".mvn/settings.xml", "-s", ".mvn/settings.xml",
                "-pl", "mq-demo", "-am", "package", "-DskipTests"
            ) `
            -WorkingDirectory $repoRoot `
            -StandardOutputPath $mavenStdout `
            -StandardErrorPath $mavenStderr `
            -TimeoutSeconds $CommandTimeoutSeconds `
            -Description "Maven build"
    }
    if (-not (Test-Path -LiteralPath $jar -PathType Leaf)) {
        throw "Demo jar not found at $jar. Build it or omit -SkipBuild."
    }

    foreach ($port in $portValues) {
        if (Test-TcpEndpoint "127.0.0.1" $port) {
            throw "Broker port $port is already accepting connections."
        }
    }

    if ($RequireUnusedComposeProject) {
        if (Test-Path -LiteralPath $composeOwnershipMarkerPath -PathType Leaf) {
            $existingMarker = Get-Content -Raw -LiteralPath $composeOwnershipMarkerPath
            if ($existingMarker -ne $expectedComposeOwnershipMarkerContent) {
                throw "RunDirectory belongs to a different compose project."
            }
        }
        if (Test-ComposeProjectHasResources) {
            throw "Compose project '$ComposeProjectName' already owns Docker resources."
        }
        $utf8 = New-Object System.Text.UTF8Encoding($false)
        [IO.File]::WriteAllText(
            $composeOwnershipMarkerPath,
            $expectedComposeOwnershipMarkerContent,
            $utf8)
    }

    $composeStdout = Join-Path $logDirectory "etcd-compose.log"
    $composeStderr = Join-Path $logDirectory "etcd-compose.error.log"
    Start-BoundedProcess `
        -FilePath "docker" `
        -ArgumentList @("compose", "--project-name", $ComposeProjectName, "-f", $composeFilePath, "up", "-d") `
        -WorkingDirectory $repoRoot `
        -StandardOutputPath $composeStdout `
        -StandardErrorPath $composeStderr `
        -TimeoutSeconds $CommandTimeoutSeconds `
        -Description "etcd compose startup"

    $endpointUris = @($EtcdEndpoints -split ',' | ForEach-Object {
            $candidate = $_.Trim()
            if ([string]::IsNullOrWhiteSpace($candidate)) {
                throw "EtcdEndpoints must not contain blank entries."
            }
            $uri = [uri]$candidate
            if ($uri.Port -lt 1) {
                throw "etcd endpoint must include a port: $candidate."
            }
            $uri
        })
    Wait-ForEtcd $endpointUris ([DateTime]::UtcNow.AddSeconds($ReadinessTimeoutSeconds))

    $brokerReadinessDeadline = [DateTime]::UtcNow.AddSeconds($ReadinessTimeoutSeconds)
    for ($brokerId = 0; $brokerId -lt 3; $brokerId++) {
        $port = $portValues[$brokerId]
        $dataDir = Join-Path $dataDirectory "broker-$brokerId"
        $logPath = Join-Path $logDirectory "broker-$brokerId.log"
        $errorLogPath = Join-Path $logDirectory "broker-$brokerId.error.log"
        New-Item -ItemType Directory -Force -Path $dataDir | Out-Null
        $arguments = @(
            "-jar", $jar,
            "broker",
            "--broker-id", [string]$brokerId,
            "--host", "127.0.0.1",
            "--port", [string]$port,
            "--data-dir", $dataDir,
            "--etcd-endpoints", $EtcdEndpoints,
            "--cluster-id", $ClusterId,
            "--admin-token", $AdminToken
        )
        if ($brokerId -eq 0) {
            $arguments += "--initialize-empty-cluster"
        }

        Write-Host "Starting broker-$brokerId on port $port..."
        $argumentLine = ($arguments | ForEach-Object { ConvertTo-NativeArgument ([string]$_) }) -join ' '
        $process = Start-Process -FilePath "java" `
            -ArgumentList $argumentLine `
            -WorkingDirectory $repoRoot `
            -RedirectStandardOutput $logPath `
            -RedirectStandardError $errorLogPath `
            -PassThru
        $process.Refresh()
        $entry = [pscustomobject][ordered]@{
            pid = $process.Id
            processStartTimeUtc = $process.StartTime.ToUniversalTime().ToString("o")
            brokerId = $brokerId
            port = $port
            logPath = $logPath
            errorLogPath = $errorLogPath
        }
        $ownedBrokers += $entry
        Write-BrokerManifest $ownedBrokers
        Wait-ForBroker $entry $brokerReadinessDeadline
    }
    Wait-ForClusterReadiness $brokerReadinessDeadline

    $bootstrapServers = ($portValues | ForEach-Object { "127.0.0.1:$_" }) -join ','
    Write-Host "All Brokers are ready."
    Write-Host "Bootstrap servers: $bootstrapServers"
    Write-Host "PID manifest: $manifestPath"
} catch {
    $failure = $_
    if ($ownedBrokers.Count -gt 0) {
        Write-Warning "Broker startup failed; stopping only processes owned by $manifestPath."
        Stop-StartedBrokers
    }
    Remove-OwnedComposeProject
    throw $failure
}
