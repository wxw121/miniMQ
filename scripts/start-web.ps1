param(
    [int]$WebPort = 18082,
    [int]$BrokerPort = 19092,
    [string]$DataDir = ".run/data-live",
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $repoRoot

if (-not $SkipBuild) {
    mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-demo -am package -DskipTests
}

$jar = Resolve-Path "mq-demo/target/mq-demo-1.2.0-SNAPSHOT.jar"
$javaArgs = @(
    "-Dorg.slf4j.simpleLogger.defaultLogLevel=info",
    "-Dorg.slf4j.simpleLogger.log.io.netty=warn",
    "-Dminimq.web.port=$WebPort",
    "-Dminimq.broker.port=$BrokerPort",
    "-Dminimq.data.dir=$DataDir",
    "-cp",
    $jar.Path,
    "com.minimq.demo.web.MiniMqWebDevMain"
)

Write-Host "MiniMQ local dev launcher"
Write-Host "Web:    http://127.0.0.1:$WebPort/"
Write-Host "Broker: 127.0.0.1:$BrokerPort"
Write-Host "Keep this terminal open while testing. Press Ctrl+C to stop."

& java @javaArgs

