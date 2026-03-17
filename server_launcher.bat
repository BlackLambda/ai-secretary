@echo off
setlocal

cd /d "%~dp0"
title AI Secretary Server

:: Find Python
set "PY_CMD="
where py >nul 2>nul
if not errorlevel 1 set "PY_CMD=py -3"
if "%PY_CMD%"=="" (
  where python >nul 2>nul
  if not errorlevel 1 set "PY_CMD=python"
)
if "%PY_CMD%"=="" (
  echo [ERROR] Python not found on PATH.
  pause
  exit /b 1
)

:: Kill any existing server on port 5000
echo [INFO] Checking for existing server on port 5000...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Get-NetTCPConnection -LocalPort 5000 -ErrorAction SilentlyContinue | Where-Object { $_.OwningProcess -ne 0 } | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }"

echo [INFO] Starting AI Secretary server...
echo        Press Ctrl+C to stop.
echo.

:: Open browser after a short delay
start "" /B powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Sleep -Seconds 3; Start-Process 'http://localhost:5000'"

%PY_CMD% "%~dp0server_react.py"

echo.
echo [INFO] Server stopped.
pause
