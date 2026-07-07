@echo off
setlocal

set "REPO_ROOT=%~dp0.."

start "MiniMQ Web Dev" /min powershell.exe -NoProfile -ExecutionPolicy Bypass -NoExit -Command "Set-Location -LiteralPath '%REPO_ROOT%'; .\scripts\start-web.ps1 -SkipBuild"

echo MiniMQ Web window started.
echo Web:    http://127.0.0.1:18082/
echo Broker: 127.0.0.1:19092
echo Stop:   scripts\stop-web.cmd
