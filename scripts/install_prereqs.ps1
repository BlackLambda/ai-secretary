$ErrorActionPreference = "Stop"

function Install-AzureCLI {
    Write-Host "[WARN] Azure CLI not found. Installing..." -ForegroundColor Yellow
    $installer = "$env:USERPROFILE\Downloads\azure-cli.msi"
    Write-Host "Downloading Azure CLI to $installer..."
    Invoke-WebRequest -Uri 'https://aka.ms/installazurecliwindows' -OutFile $installer
    
    Write-Host "Installing Azure CLI (Check for UAC prompt)..."
    Start-Process msiexec.exe -Wait -Verb RunAs -ArgumentList "/i `"$installer`" /quiet"
    Write-Host "[INFO] Azure CLI installed." -ForegroundColor Green
}

function Install-Python {
    Write-Host "[WARN] Python 3.11 not found. Installing Python 3.11 for the current user..." -ForegroundColor Yellow
    $installer = "$env:USERPROFILE\Downloads\python-installer.exe"
    Write-Host "Downloading Python to $installer..."
    Invoke-WebRequest -Uri 'https://www.python.org/ftp/python/3.11.6/python-3.11.6-amd64.exe' -OutFile $installer
    
    Write-Host "Installing Python for the current user..."
    Start-Process -FilePath $installer -Wait -ArgumentList "/quiet InstallAllUsers=0 PrependPath=1 Include_launcher=1 Include_pip=1"
    Write-Host "[INFO] Python installed." -ForegroundColor Green
}

function Install-NodeJS {
    Write-Host "[WARN] Node.js/npm not found. Installing Node.js LTS..." -ForegroundColor Yellow
    $installer = "$env:USERPROFILE\Downloads\node-installer.msi"
    Write-Host "Downloading Node.js to $installer..."
    Invoke-WebRequest -Uri 'https://nodejs.org/dist/v20.10.0/node-v20.10.0-x64.msi' -OutFile $installer
    
    Write-Host "Installing Node.js (Check for UAC prompt)..."
    Start-Process msiexec.exe -Wait -Verb RunAs -ArgumentList "/i `"$installer`" /quiet"
    Write-Host "[INFO] Node.js installed." -ForegroundColor Green
}

# 1. Check Azure CLI (optional)
Write-Host "[Pre-check] Verifying Azure CLI (optional)..."
if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
    Write-Host "[INFO] Azure CLI not found. Continuing without it." -ForegroundColor Yellow
}

# 2. Check Python 3.11
Write-Host "[Pre-check] Verifying Python 3.11..."
$needsPython = $false
try {
    $v = $null
    if (Get-Command py -ErrorAction SilentlyContinue) {
        try { $v = & py -3.11 --version 2>&1 } catch { $v = $null }
    }
    if (-not $v -and (Get-Command python -ErrorAction SilentlyContinue)) {
        try { $v = & python --version 2>&1 } catch { $v = $null }
    }
    if (-not $v) {
        $needsPython = $true
        throw "Python not found"
    }
    if ($v -match 'Python (\d+)\.(\d+)') {
        $maj = [int]$matches[1]
        $min = [int]$matches[2]
        if ($maj -ne 3 -or $min -ne 11) {
            $needsPython = $true
        }
    } else {
        $needsPython = $true
    }
} catch {
    $needsPython = $true
}

if ($needsPython) {
    Write-Host "[WARN] Python 3.11 is not installed or not found in PATH." -ForegroundColor Yellow
    $response = Read-Host "Do you want to automatically install Python 3.11? (Y/N)"
    if ($response -match "^[Yy]") {
        Install-Python
        Write-Host ""
        Write-Host "================================================================" -ForegroundColor Cyan
        Write-Host "Python installation complete." -ForegroundColor Cyan
        Write-Host "PLEASE RESTART YOUR TERMINAL/POWERSHELL WINDOW NOW." -ForegroundColor Cyan
        Write-Host "Then run start.bat again." -ForegroundColor Cyan
        Write-Host "================================================================" -ForegroundColor Cyan
        exit 100 # Special code for "Installed, need restart"
    } else {
        Write-Host "[ERROR] Python 3.11 is required to run this application. Please install it manually." -ForegroundColor Red
        exit 1
    }
}

# 3. Check Node.js/npm
Write-Host "[Pre-check] Verifying Node.js/npm..."
if (-not (Get-Command npm -ErrorAction SilentlyContinue)) {
    Write-Host "[WARN] Node.js/npm is not installed or not found in PATH." -ForegroundColor Yellow
    $response = Read-Host "Do you want to automatically install Node.js? (Y/N)"
    if ($response -match "^[Yy]") {
        Install-NodeJS
        Write-Host ""
        Write-Host "================================================================" -ForegroundColor Cyan
        Write-Host "Node.js installation complete." -ForegroundColor Cyan
        Write-Host "PLEASE RESTART YOUR TERMINAL/POWERSHELL WINDOW NOW." -ForegroundColor Cyan
        Write-Host "Then run start.bat again." -ForegroundColor Cyan
        Write-Host "================================================================" -ForegroundColor Cyan
        exit 100 # Special code for "Installed, need restart"
    } else {
        Write-Host "[ERROR] Node.js/npm is required for the frontend. Please install it manually from https://nodejs.org/" -ForegroundColor Red
        exit 1
    }
}

exit 0
