[CmdletBinding()]
param(
    [string[]]$Ports = @("9092", "9093", "9094"),
    [string]$EtcdEndpoints = "http://localhost:2379",
    [string]$ClusterId = "minimq",
    [string]$DataRoot = "./data",
    [string]$AdminToken = "demo-admin",
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"

$portValues = @()
foreach ($portEntry in $Ports) {
    foreach ($part in ($portEntry -split ",")) {
        if (-not [string]::IsNullOrWhiteSpace($part)) {
            $portValues += [int]$part.Trim()
        }
    }
}

if ($portValues.Count -ne 3) {
    throw "Ports must contain exactly three values (one per broker)."
}
if (($portValues | Select-Object -Unique).Count -ne 3 -or ($portValues | Where-Object {
        $_ -lt 1 -or $_ -gt 65535
    }).Count -ne 0) {
    throw "Ports must be three unique values between 1 and 65535."
}
if ([string]::IsNullOrWhiteSpace($AdminToken)) {
    throw "AdminToken must not be blank."
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $repoRoot
$pom = [xml](Get-Content -Raw (Join-Path $repoRoot "pom.xml"))
$version = $pom.project.version
$jar = Join-Path $repoRoot "mq-demo\target\mq-demo-$version.jar"

if (-not $SkipBuild) {
    mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-demo -am package -DskipTests
}
if (-not (Test-Path -LiteralPath $jar)) {
    throw "Demo jar not found at $jar. Build with: mvn -pl mq-demo -am package -gs .mvn/settings.xml -s .mvn/settings.xml"
}

$dataRootPath = if ([IO.Path]::IsPathRooted($DataRoot)) {
    [IO.Path]::GetFullPath($DataRoot)
} else {
    [IO.Path]::GetFullPath((Join-Path $repoRoot $DataRoot))
}
New-Item -ItemType Directory -Force -Path $dataRootPath | Out-Null

for ($brokerId = 0; $brokerId -lt 3; $brokerId++) {
    $port = $portValues[$brokerId]
    $dataDir = Join-Path $dataRootPath "broker-$brokerId"
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

    Write-Host "Starting broker-$brokerId on $port..."
    Start-Process -FilePath "java" -ArgumentList $arguments -WorkingDirectory $repoRoot | Out-Null
    if ($brokerId -eq 0) {
        Start-Sleep -Seconds 2
    } else {
        Start-Sleep -Seconds 1
    }
}

Write-Host "All brokers started."
