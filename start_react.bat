@echo off
REM Quick start script for AI Secretary React app

echo Starting AI Secretary React App...
echo.

REM Resolve Python command once and use it consistently.
set "PY_CMD="
where py >nul 2>nul
if not errorlevel 1 set "PY_CMD=py -3"
if "%PY_CMD%"=="" (
    where python >nul 2>nul
    if not errorlevel 1 set "PY_CMD=python"
)
if "%PY_CMD%"=="" (
    echo [ERROR] Python was not found on PATH.
    echo Please install Python 3 ^(or the Python Launcher 'py'^) and reopen this terminal.
    pause
    exit /b 1
)
echo [INFO] Using: %PY_CMD%

REM Prevent multiple app instances
powershell -NoProfile -ExecutionPolicy Bypass -Command "try { $already = $false; $serverProc = @(Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -and ($_.Name -ieq 'python.exe' -or $_.Name -ieq 'pythonw.exe' -or $_.Name -ieq 'python') -and $_.CommandLine -like '*server_react.py*' }); if ($serverProc.Count -gt 0) { $already = $true }; function Get-ListenPids([int]$Port) { try { return (Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction Stop | Select-Object -ExpandProperty OwningProcess -Unique) } catch { $lines = (netstat -ano -p tcp | Select-String -Pattern (':'+$Port+' ') -SimpleMatch); $pids = @(); foreach ($l in $lines) { $t = ($l.ToString() -replace '\s+', ' ').Trim().Split(' '); if ($t.Length -ge 5 -and $t[3] -eq 'LISTENING') { $pids += [int]$t[4] } }; return ($pids | Select-Object -Unique) } }; $listenPids = @(Get-ListenPids 5000); if ($listenPids.Count -gt 0) { $already = $true }; if ($already) { Write-Host '[ERROR] AI Secretary backend appears to already be running (server_react.py / port 5000 in use).' -ForegroundColor Red; if ($serverProc.Count -gt 0) { Write-Host 'Detected python process(es) running server_react.py:'; foreach ($sp in ($serverProc | Select-Object -First 5)) { $cl = [string]$sp.CommandLine; if ([string]::IsNullOrWhiteSpace($cl)) { $cl = '<unknown>' } elseif ($cl.Length -gt 180) { $cl = $cl.Substring(0,180) + '...' }; Write-Host ((' - PID {0}: {1}' -f $sp.ProcessId, $cl)) } }; foreach ($procId in $listenPids) { $cmd = ''; try { $p = Get-CimInstance Win32_Process -Filter (\"ProcessId=$procId\") -ErrorAction Stop; $cmd = [string]$p.CommandLine } catch { $cmd = '' }; if ([string]::IsNullOrWhiteSpace($cmd)) { $cmd = '<unknown>' } elseif ($cmd.Length -gt 180) { $cmd = $cmd.Substring(0,180) + '...' }; Write-Host ((' - Port 5000 (PID {0}): {1}' -f $procId, $cmd)) }; Write-Host 'Close the existing Flask server window (or free port 5000) and try again.'; exit 20 } } catch { Write-Host ('[WARN] Instance detection failed: ' + $_.Exception.Message) -ForegroundColor Yellow }"
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [ERROR] Another instance is running. Exiting.
    pause
    exit /b %ERRORLEVEL%
)

REM Check if node_modules exists
if not exist "frontend\node_modules" (
    echo Installing frontend dependencies...
    cd frontend
    call npm install
    cd ..
    echo Dependencies installed!
    echo.
)

echo Starting development servers...
echo.
echo Flask API: http://localhost:5000
echo React App: http://localhost:3000
echo.
echo Tip: Open http://localhost:3000 in your browser
echo Tip: Press Ctrl+C to stop
echo.

REM Start Flask server
if "%1"=="" (
    start "Flask Server" cmd /k %PY_CMD% server_react.py
) else (
    start "Flask Server" cmd /k %PY_CMD% server_react.py --data %1
)

REM Wait a moment for Flask to start
timeout /t 2 /nobreak >nul

REM Start React dev server
cd frontend
call npm run dev -- --open
cd ..
