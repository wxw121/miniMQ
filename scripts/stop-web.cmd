@echo off
setlocal

set "TASK_NAME=MiniMQWebDev"

schtasks.exe /End /TN "%TASK_NAME%" >nul 2>nul
schtasks.exe /Delete /TN "%TASK_NAME%" /F >nul 2>nul

powershell.exe -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ports = @(18082,19092); foreach ($port in $ports) { Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique | ForEach-Object { Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue } }"

echo MiniMQ Web stopped.
