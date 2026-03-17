@echo off
REM Transcript Monitor Starter Script
REM ==================================
REM 
REM This script starts the transcript monitor in the background.
REM The monitor will run continuously, checking for new transcripts every 5 minutes.
REM
REM Usage:
REM   start_monitor.bat           - Start monitor in new window
REM   start_monitor.bat hidden    - Start monitor hidden (background)

echo ========================================
echo Transcript Monitor Starter
echo ========================================
echo.

cd /d %~dp0

if "%1"=="hidden" (
    echo Starting monitor in hidden mode...
    start /B pythonw transcript_monitor.py
    echo Monitor started in background.
    echo Check logs\transcript_monitor.log for activity.
) else (
    echo Starting monitor in new window...
    start "Transcript Monitor" python transcript_monitor.py
    echo Monitor started in new window.
    echo Press Ctrl+C in that window to stop.
)

echo.
echo ========================================
pause
