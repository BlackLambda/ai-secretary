@echo off
setlocal EnableExtensions EnableDelayedExpansion

cd /d "%~dp0"
title AI Secretary Launcher

echo ========================================================
echo AI Secretary - Quick Start
echo ========================================================

:: Register ai-secretary:// protocol handler so the browser extension can start the server
echo.
echo [Pre-check] Registering ai-secretary:// protocol handler...
reg add "HKCU\Software\Classes\ai-secretary" /ve /d "URL:AI Secretary Protocol" /f >nul 2>&1
reg add "HKCU\Software\Classes\ai-secretary" /v "URL Protocol" /d "" /f >nul 2>&1
reg add "HKCU\Software\Classes\ai-secretary\shell\open\command" /ve /d "\"%~dp0server_launcher.bat\"" /f >nul 2>&1
echo [INFO] Protocol handler registered.

:: Register ai-secretary-update:// protocol to trigger start.bat (full update + restart)
reg add "HKCU\Software\Classes\ai-secretary-update" /ve /d "URL:AI Secretary Update Protocol" /f >nul 2>&1
reg add "HKCU\Software\Classes\ai-secretary-update" /v "URL Protocol" /d "" /f >nul 2>&1
reg add "HKCU\Software\Classes\ai-secretary-update\shell\open\command" /ve /d "\"%~dp0start.bat\"" /f >nul 2>&1
echo [INFO] Update protocol handler registered.

:: Kill any existing AI Secretary server on port 5000 (for update-restart flow)
echo.
echo [Pre-check] Stopping any existing server on port 5000...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Get-NetTCPConnection -LocalPort 5000 -ErrorAction SilentlyContinue | Where-Object { $_.OwningProcess -ne 0 } | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }"

:: Pull latest changes
echo.
echo [Pre-check] Pulling latest changes...
git pull
if %ERRORLEVEL% NEQ 0 (
  echo [WARN] Failed to pull latest changes. Continuing with local version...
)

:: SubstrateDataExtraction is vendored in this repo; do not treat it as a nested git repo.
echo.
echo [Pre-check] Verifying vendored SubstrateDataExtraction folder...
if exist "%~dp0SubstrateDataExtraction" (
  echo [INFO] Found vendored SubstrateDataExtraction source.
) else (
  echo [ERROR] Missing vendored SubstrateDataExtraction folder at "%~dp0SubstrateDataExtraction"
  echo Restore the folder from this repository before starting the app.
  pause
  exit /b 1
)

:: Check Prerequisites (Python 3.11 and Node.js; Azure CLI optional)
echo.
echo [Pre-check] Verifying Python 3.11 / Node.js prerequisites...
powershell -ExecutionPolicy Bypass -File "%~dp0scripts\install_prereqs.ps1"
if %ERRORLEVEL% EQU 100 (
  echo.
  echo [IMPORTANT] Prerequisite installation completed. Reloading environment...
  timeout /t 3 >nul
  powershell -NoProfile -ExecutionPolicy Bypass -Command "$env:Path = [System.Environment]::GetEnvironmentVariable('Path','Machine') + ';' + [System.Environment]::GetEnvironmentVariable('Path','User'); Start-Process -FilePath 'cmd.exe' -ArgumentList '/c', '""%~f0""'"
  exit /b
)
if %ERRORLEVEL% NEQ 0 (
  echo [ERROR] Prerequisites check failed.
  pause
  exit /b 1
)

:: Resolve Python 3.11 bootstrap command.
echo.
echo [Pre-check] Resolving Python 3.11...
set "PY311_CMD="
where py >nul 2>nul
if not errorlevel 1 (
  py -3.11 --version >nul 2>nul
  if not errorlevel 1 set "PY311_CMD=py -3.11"
)
if "%PY311_CMD%"=="" (
  if exist "%LOCALAPPDATA%\Programs\Python\Python311\python.exe" set "PY311_CMD=%LOCALAPPDATA%\Programs\Python\Python311\python.exe"
)
if "%PY311_CMD%"=="" (
  where python >nul 2>nul
  if not errorlevel 1 (
    for /f "tokens=2 delims= " %%v in ('python --version 2^>^&1') do set "PY_VERSION=%%v"
    if "%PY_VERSION:~0,4%"=="3.11" set "PY311_CMD=python"
  )
)
if "%PY311_CMD%"=="" (
  echo [ERROR] Python 3.11 was not found on PATH.
  echo Please install Python 3.11 and reopen this terminal.
  pause
  exit /b 1
)
echo [INFO] Using bootstrap Python: %PY311_CMD%

set "VENV_DIR=%~dp0.venv"
set "VENV_PY=%VENV_DIR%\Scripts\python.exe"
set "VENV_CFG=%VENV_DIR%\pyvenv.cfg"
set "REBUILD_VENV=0"
if exist "%VENV_PY%" (
  set "VENV_VERSION="
  if exist "%VENV_CFG%" (
    for /f "tokens=2 delims==" %%v in ('findstr /b /c:"version =" "%VENV_CFG%"') do set "VENV_VERSION=%%v"
    if defined VENV_VERSION set "VENV_VERSION=!VENV_VERSION: =!"
    if defined VENV_VERSION set "VENV_VERSION=!VENV_VERSION:~0,4!"
  )
  if not "!VENV_VERSION!"=="3.11" (
    echo [WARN] Existing .venv uses Python !VENV_VERSION!. Rebuilding with Python 3.11...
    set "REBUILD_VENV=1"
  ) else (
    echo [INFO] Reusing existing Python 3.11 virtual environment.
  )
) else (
  echo [INFO] No .venv found. Creating a new Python 3.11 virtual environment...
  set "REBUILD_VENV=1"
)

if "%REBUILD_VENV%"=="1" (
  if exist "%VENV_DIR%" rmdir /s /q "%VENV_DIR%"
  echo.
  echo [Step 1] Creating Python 3.11 virtual environment...
  call %PY311_CMD% -m venv "%VENV_DIR%"
  if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to create .venv with Python 3.11.
    pause
    exit /b %ERRORLEVEL%
  )
)

if not exist "%VENV_PY%" (
  echo [ERROR] Virtual environment python not found at "%VENV_PY%"
  pause
  exit /b 1
)

set "PY_CMD=%VENV_PY%"
echo [INFO] Using virtual environment: %VENV_DIR%

set "DEPS_SCRIPT=%~dp0scripts\python_dependency_state.py"
set "DEPS_STAMP=%VENV_DIR%\.deps-fingerprint"
set "DEPS_FINGERPRINT_TMP=%VENV_DIR%\.deps-fingerprint.tmp"
set "CURRENT_DEPS_FINGERPRINT="
"%PY_CMD%" "%DEPS_SCRIPT%" --fingerprint > "%DEPS_FINGERPRINT_TMP%"
if %ERRORLEVEL% NEQ 0 (
  echo [ERROR] Failed to compute Python dependency fingerprint.
  pause
  exit /b %ERRORLEVEL%
)
set /p CURRENT_DEPS_FINGERPRINT=<"%DEPS_FINGERPRINT_TMP%"
del /q "%DEPS_FINGERPRINT_TMP%" >nul 2>nul

set "INSTALL_DEPS=0"
if "%REBUILD_VENV%"=="1" (
  set "INSTALL_DEPS=1"
) else if not exist "%DEPS_STAMP%" (
  echo [INFO] No dependency stamp found. Installing Python dependencies...
  set "INSTALL_DEPS=1"
) else (
  set /p SAVED_DEPS_FINGERPRINT=<"%DEPS_STAMP%"
  if not "%SAVED_DEPS_FINGERPRINT%"=="%CURRENT_DEPS_FINGERPRINT%" (
    echo [INFO] Python dependency manifests changed. Installing updates...
    set "INSTALL_DEPS=1"
  ) else (
    echo [INFO] Python dependency manifests unchanged. Skipping pip install.
  )
)

if "%INSTALL_DEPS%"=="0" (
  "%PY_CMD%" "%DEPS_SCRIPT%" --check
  if %ERRORLEVEL% NEQ 0 (
    echo [INFO] Installed packages do not satisfy the current manifests. Installing updates...
    set "INSTALL_DEPS=1"
  ) else (
    echo [INFO] Installed packages satisfy the current manifests.
  )
)

echo.
if "%INSTALL_DEPS%"=="1" (
  echo [Step 2] Installing/Updating Virtual Environment Dependencies...
  call "%PY_CMD%" -m pip install --upgrade pip
  if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to upgrade pip in .venv.
    pause
    exit /b %ERRORLEVEL%
  )
  call "%PY_CMD%" -m pip install ".[desktop]"
  if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to install desktop dependencies into .venv ^(.[desktop]^).
    pause
    exit /b %ERRORLEVEL%
  )
  >"%DEPS_STAMP%" echo %CURRENT_DEPS_FINGERPRINT%
) else (
  echo [Step 2] Python dependencies already up to date. Skipping install.
)

echo.
echo [Step 3] Preparing Runtime...

if not exist "%~dp0frontend\package.json" (
  echo [ERROR] Frontend package.json not found at "%~dp0frontend\package.json"
  pause
  exit /b 1
)

if not exist "%~dp0frontend\node_modules" (
  echo [INFO] Frontend dependencies missing. Installing with npm ci...
  pushd "%~dp0frontend"
  call npm ci
  set "NPM_EXIT_CODE=%ERRORLEVEL%"
  popd
  if not "!NPM_EXIT_CODE!"=="0" (
    echo [ERROR] Failed to install frontend dependencies.
    pause
    exit /b !NPM_EXIT_CODE!
  )
)

echo [INFO] Building frontend into static\app...
pushd "%~dp0frontend"
call npm run build
set "FRONTEND_BUILD_EXIT_CODE=%ERRORLEVEL%"
popd
if not "!FRONTEND_BUILD_EXIT_CODE!"=="0" (
  echo [ERROR] Frontend build failed.
  pause
  exit /b !FRONTEND_BUILD_EXIT_CODE!
)

echo.
echo [Step 4] Starting Server + Browser...
echo.

echo [Pre-step] Stopping any running incremental pipeline processes...
powershell -NoProfile -ExecutionPolicy Bypass -Command "try { $procs = Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -and $_.CommandLine -like '*run_incremental_pipeline.py*' }; if ($procs) { $pids = $procs | Select-Object -ExpandProperty ProcessId; Write-Host ('Found pipeline PIDs: ' + ($pids -join ', ')); $pids | ForEach-Object { Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue }; } else { Write-Host 'No running pipeline process found.' } } catch { Write-Host ('[WARN] Failed to enumerate/stop pipeline processes: ' + $_.Exception.Message) }"

if not exist "%~dp0server_react.py" (
  echo [ERROR] server_react.py not found at "%~dp0server_react.py"
  pause
  exit /b 1
)

echo [INFO] Launching server and opening dashboard in browser...
echo [INFO] Command: "%PY_CMD%" server_react.py
echo.
echo [INFO] Starting server (foreground)...
echo        Press Ctrl+C in this terminal to stop the server.
echo.

:: Open the dashboard in the default browser after a short delay (gives server time to start).
start "" /B powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Sleep -Seconds 3; Start-Process 'http://localhost:5000'"

call "%PY_CMD%" "%~dp0server_react.py"
set "APP_EXIT_CODE=%ERRORLEVEL%"
echo.
echo [INFO] Server exited with code %APP_EXIT_CODE%.
if %APP_EXIT_CODE% NEQ 0 (
  echo [ERROR] Server failed to start or crashed.
  pause
  exit /b %APP_EXIT_CODE%
)

endlocal
exit /b 0
