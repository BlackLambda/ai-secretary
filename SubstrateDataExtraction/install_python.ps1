# install_python.ps1 - Automated Python Installer for Windows
# Compatible with: Windows 10, Windows 11, Windows Server
# Architectures: AMD64 (x64), ARM64, x86 (32-bit)
#
# USAGE:
#   Run as Administrator: Right-click PowerShell -> Run as Administrator
#   Then: .\install_python.ps1

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Python Auto-Installer for Windows" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if running as Administrator
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "[WARNING] Not running as Administrator!" -ForegroundColor Yellow
    Write-Host "          Some features may not work properly." -ForegroundColor Yellow
    Write-Host "          Recommended: Right-click PowerShell -> Run as Administrator" -ForegroundColor Yellow
    Write-Host ""
    $continue = Read-Host "Continue anyway? (y/N)"
    if ($continue -ne "y" -and $continue -ne "Y") {
        Write-Host "Installation cancelled." -ForegroundColor Red
        exit 1
    }
    Write-Host ""
}

# Step 1: Detect System Information
Write-Host "[1/7] Detecting system information..." -ForegroundColor Yellow

# Get Windows version
$osInfo = Get-CimInstance -ClassName Win32_OperatingSystem
$osVersion = $osInfo.Caption
$osBuild = $osInfo.BuildNumber
Write-Host "      OS: $osVersion (Build $osBuild)" -ForegroundColor Gray

# Determine if Windows 11 (build 22000+)
$isWin11 = $osBuild -ge 22000
if ($isWin11) {
    Write-Host "      Detected: Windows 11" -ForegroundColor Green
} else {
    Write-Host "      Detected: Windows 10 or earlier" -ForegroundColor Green
}

# Get processor architecture
$arch = $env:PROCESSOR_ARCHITECTURE
$procInfo = Get-CimInstance -ClassName Win32_Processor | Select-Object -First 1
$procName = $procInfo.Name

Write-Host "      Processor: $procName" -ForegroundColor Gray
Write-Host "      Architecture: $arch" -ForegroundColor Gray

# Additional check for ARM64 on x64 emulation
if ($arch -eq "AMD64" -and $procName -match "ARM") {
    Write-Host "      Note: ARM processor detected, but running x64 emulation" -ForegroundColor Yellow
    $arch = "ARM64"
}

Write-Host ""

# Step 2: Determine Python Installer
Write-Host "[2/7] Selecting appropriate Python installer..." -ForegroundColor Yellow

# Python version (use latest stable)
$pythonVersion = "3.11.9"

# Select installer based on architecture
$installerType = ""
switch ($arch) {
    "AMD64" {
        $python_url = "https://www.python.org/ftp/python/$pythonVersion/python-$pythonVersion-amd64.exe"
        $installerType = "AMD64 (64-bit Intel/AMD)"
    }
    "ARM64" {
        $python_url = "https://www.python.org/ftp/python/$pythonVersion/python-$pythonVersion-arm64.exe"
        $installerType = "ARM64 (64-bit ARM)"
    }
    "x86" {
        $python_url = "https://www.python.org/ftp/python/$pythonVersion/python-$pythonVersion.exe"
        $installerType = "x86 (32-bit)"
    }
    default {
        Write-Error "Unsupported architecture: $arch"
        Write-Host "      Supported: AMD64, ARM64, x86" -ForegroundColor Red
        exit 1
    }
}

Write-Host "      Python Version: $pythonVersion" -ForegroundColor Green
Write-Host "      Installer Type: $installerType" -ForegroundColor Green
Write-Host "      Download URL: $python_url" -ForegroundColor Gray
Write-Host ""

# Step 3: Check if Python already exists
Write-Host "[3/7] Checking existing Python installations..." -ForegroundColor Yellow

$existingPython = Get-Command python -ErrorAction SilentlyContinue
if ($existingPython) {
    $currentVersion = & python --version 2>&1
    Write-Host "      Found: $currentVersion" -ForegroundColor Yellow
    Write-Host "      Location: $($existingPython.Source)" -ForegroundColor Gray
    Write-Host ""
    $overwrite = Read-Host "Python is already installed. Continue anyway? (y/N)"
    if ($overwrite -ne "y" -and $overwrite -ne "Y") {
        Write-Host "Installation cancelled." -ForegroundColor Red
        exit 0
    }
} else {
    Write-Host "      No existing Python installation found" -ForegroundColor Green
}
Write-Host ""

# Step 4: Download Python Installer
Write-Host "[4/7] Downloading Python installer..." -ForegroundColor Yellow

$installer = "$env:TEMP\python_installer_$pythonVersion.exe"
Write-Host "      Saving to: $installer" -ForegroundColor Gray

try {
    # Download with progress bar
    $ProgressPreference = 'SilentlyContinue'
    Invoke-WebRequest -Uri $python_url -OutFile $installer -UseBasicParsing
    $ProgressPreference = 'Continue'

    if (!(Test-Path $installer)) {
        throw "File not found after download"
    }

    $fileSize = (Get-Item $installer).Length / 1MB
    Write-Host "      Downloaded: $([math]::Round($fileSize, 2)) MB" -ForegroundColor Green
} catch {
    Write-Error "Download failed: $_"
    Write-Host "      Please check your internet connection" -ForegroundColor Red
    exit 1
}
Write-Host ""

# Step 5: Install Python
Write-Host "[5/7] Installing Python (this may take a few minutes)..." -ForegroundColor Yellow

# Installation arguments
# - /quiet: Silent installation
# - InstallAllUsers=1: Install for all users (requires admin)
# - PrependPath=1: Add Python to PATH
# - Include_pip=1: Include pip package manager
# - Include_launcher=1: Include py launcher
# - Include_test=0: Don't include test suite (saves space)

$installArgs = "/quiet InstallAllUsers=1 PrependPath=1 Include_pip=1 Include_launcher=1 Include_test=0"

Write-Host "      Running installer with arguments: $installArgs" -ForegroundColor Gray

try {
    $process = Start-Process -FilePath $installer -ArgumentList $installArgs -Wait -PassThru -NoNewWindow

    if ($process.ExitCode -eq 0) {
        Write-Host "      Installation completed successfully" -ForegroundColor Green
    } else {
        Write-Warning "      Installer returned exit code: $($process.ExitCode)"
        Write-Host "      Python may still be installed correctly" -ForegroundColor Yellow
    }
} catch {
    Write-Error "Installation failed: $_"
    exit 1
}

# Clean up installer
Remove-Item $installer -Force -ErrorAction SilentlyContinue
Write-Host ""

# Step 6: Update Environment Variables
Write-Host "[6/7] Updating environment variables..." -ForegroundColor Yellow

# Refresh environment variables in current session
$env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path", "User")

Write-Host "      PATH updated for current session" -ForegroundColor Green
Write-Host "      Note: You may need to restart terminal for changes to take effect globally" -ForegroundColor Yellow
Write-Host ""

# Step 7: Verify Installation
Write-Host "[7/7] Verifying Python installation..." -ForegroundColor Yellow

Start-Sleep -Seconds 2  # Give system time to update

# Check Python
$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if ($pythonCmd) {
    $installedVersion = & python --version 2>&1
    Write-Host "      [OK] Python: $installedVersion" -ForegroundColor Green
    Write-Host "           Location: $($pythonCmd.Source)" -ForegroundColor Gray
} else {
    Write-Host "      [WARN] Python command not found in PATH" -ForegroundColor Yellow
    Write-Host "           You may need to restart your terminal" -ForegroundColor Yellow
}

# Check pip
$pipCmd = Get-Command pip -ErrorAction SilentlyContinue
if ($pipCmd) {
    $pipVersion = & pip --version 2>&1
    Write-Host "      [OK] pip: $pipVersion" -ForegroundColor Green
} else {
    Write-Host "      [WARN] pip command not found" -ForegroundColor Yellow
}

# Check py launcher
$pyCmd = Get-Command py -ErrorAction SilentlyContinue
if ($pyCmd) {
    Write-Host "      [OK] py launcher: Available" -ForegroundColor Green
} else {
    Write-Host "      [WARN] py launcher not found" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Installation Complete!" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Next steps
Write-Host "Next Steps:" -ForegroundColor Yellow
Write-Host "  1. Close and reopen your terminal (if commands not found)" -ForegroundColor White
Write-Host "  2. Verify: python --version" -ForegroundColor White
Write-Host "  3. Install dependencies: pip install requests msal PyJWT" -ForegroundColor White
Write-Host "  4. Run the application: python main.py --help" -ForegroundColor White
Write-Host ""

# Test command suggestions
Write-Host "Quick Test:" -ForegroundColor Yellow
Write-Host '  python -c "print(''Hello from Python!'')"' -ForegroundColor Gray
Write-Host ""
