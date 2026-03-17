@echo off
setlocal

cd /d "%~dp0"

if not exist "%~dp0.venv\Scripts\activate.bat" (
  echo [ERROR] Virtual environment not found at "%~dp0.venv\Scripts\activate.bat"
  echo Run start.bat first to create the Python 3.11 .venv.
  exit /b 1
)

endlocal & call "%~dp0.venv\Scripts\activate.bat"