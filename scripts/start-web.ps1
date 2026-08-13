param(
    [int]$WebPort = 18082,
    [int]$BrokerPort = 19092,
    [string]$DataDir = ".run/data-live",
    [string]$AdminToken = $env:MINIMQ_ADMIN_TOKEN,
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($AdminToken)) {
    throw "AdminToken must not be blank. Pass -AdminToken or set MINIMQ_ADMIN_TOKEN."
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $repoRoot
$pom = [xml](Get-Content -Raw (Join-Path $repoRoot "pom.xml"))
$version = $pom.project.version

if (-not $SkipBuild) {
    mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-demo -am package -DskipTests
}

$jarPath = Join-Path $repoRoot "mq-demo/target/mq-demo-$version.jar"
if (-not (Test-Path -LiteralPath $jarPath)) {
    throw "Demo jar not found at $jarPath. Build with: mvn -pl mq-demo -am package -gs .mvn/settings.xml -s .mvn/settings.xml"
}
$jar = (Resolve-Path -LiteralPath $jarPath).Path
$javaArgs = @(
    "-Dorg.slf4j.simpleLogger.defaultLogLevel=info",
    "-Dorg.slf4j.simpleLogger.log.io.netty=warn",
    "-Dminimq.web.port=$WebPort",
    "-Dminimq.broker.port=$BrokerPort",
    "-Dminimq.data.dir=$DataDir",
    "-Dminimq.admin.token=$AdminToken",
    "-cp",
    $jar,
    "com.minimq.demo.web.MiniMqWebDevMain"
)

Write-Host "MiniMQ local dev launcher"
Write-Host "Web:    http://127.0.0.1:$WebPort/"
Write-Host "Broker: 127.0.0.1:$BrokerPort"
Write-Host "Keep this terminal open while testing. Press Ctrl+C to stop."

& java @javaArgs
