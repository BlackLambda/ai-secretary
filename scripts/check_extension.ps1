<#
.SYNOPSIS
    Check if AI Secretary extension is installed in Edge browser.
    
.DESCRIPTION
    Reads Edge Preferences file to verify if the extension is registered.
    Displays installation status and details.
#>

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  AI Secretary - Extension Status Check    " -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# Find extension folder
$ExtDir = Join-Path $PSScriptRoot "browser_extension"
if (-not (Test-Path $ExtDir)) {
    Write-Host "[ERROR] Extension folder not found: $ExtDir" -ForegroundColor Red
    exit 1
}

# Read manifest for expected info
$ManifestPath = Join-Path $ExtDir "manifest.json"
try {
    $Manifest = Get-Content $ManifestPath -Raw | ConvertFrom-Json
    $ExpectedName = $Manifest.name
    $ExpectedVersion = $Manifest.version
    Write-Host "Looking for: $ExpectedName v$ExpectedVersion" -ForegroundColor Gray
    Write-Host "Expected path: $ExtDir" -ForegroundColor Gray
    Write-Host ""
} catch {
    Write-Host "[ERROR] Failed to read manifest.json: $_" -ForegroundColor Red
    exit 1
}

# Find Edge Preferences
$PrefsPath = Join-Path $env:LOCALAPPDATA "Microsoft\Edge\User Data\Default\Preferences"
if (-not (Test-Path $PrefsPath)) {
    Write-Host "[WARN] Edge Preferences not found." -ForegroundColor Yellow
    Write-Host "       Edge may not be installed or hasn't been run yet." -ForegroundColor Gray
    Write-Host ""
    Write-Host "Status: NOT INSTALLED ❌" -ForegroundColor Red
    exit 0
}

# Load Preferences
try {
    $Prefs = Get-Content $PrefsPath -Raw | ConvertFrom-Json
} catch {
    Write-Host "[ERROR] Failed to parse Edge Preferences: $_" -ForegroundColor Red
    exit 1
}

# Check if extensions.settings exists
if (-not $Prefs.extensions -or -not $Prefs.extensions.settings) {
    Write-Host "No extensions registered in Edge." -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Status: NOT INSTALLED ❌" -ForegroundColor Red
    exit 0
}

# Generate expected extension ID
$NormPath = $ExtDir.ToLower().Replace("\", "/")
$PathBytes = [System.Text.Encoding]::UTF8.GetBytes($NormPath)
$Hash = [System.Security.Cryptography.SHA256]::Create().ComputeHash($PathBytes)
$ExpectedId = -join ($Hash[0..15] | ForEach-Object { [char]([int][char]'a' + ($_ % 16)) })

# Search for extension by ID or path
$Found = $null
$FoundId = $null

# First check by expected ID
if ($Prefs.extensions.settings.PSObject.Properties.Name -contains $ExpectedId) {
    $Found = $Prefs.extensions.settings.$ExpectedId
    $FoundId = $ExpectedId
} else {
    # Search by path (case insensitive)
    foreach ($prop in $Prefs.extensions.settings.PSObject.Properties) {
        $ext = $prop.Value
        if ($ext.path -and $ext.path.ToLower().Replace("\", "/") -eq $NormPath) {
            $Found = $ext
            $FoundId = $prop.Name
            break
        }
    }
}

if ($Found) {
    Write-Host "Status: INSTALLED ✅" -ForegroundColor Green
    Write-Host ""
    Write-Host "Extension Details:" -ForegroundColor Cyan
    Write-Host "  ID:       $FoundId" -ForegroundColor White
    Write-Host "  Name:     $($Found.manifest.name)" -ForegroundColor White
    Write-Host "  Version:  $($Found.manifest.version)" -ForegroundColor White
    Write-Host "  Path:     $($Found.path)" -ForegroundColor White
    
    $State = switch ($Found.state) {
        0 { "Disabled 🔴"; $color = "Red" }
        1 { "Enabled 🟢"; $color = "Green" }
        2 { "Terminated"; $color = "Yellow" }
        3 { "Blacklisted"; $color = "Red" }
        4 { "Blocklisted"; $color = "Red" }
        default { "Unknown ($($Found.state))"; $color = "Gray" }
    }
    Write-Host "  State:    $State" -ForegroundColor $color
    
    $Location = switch ($Found.location) {
        0 { "Internal" }
        1 { "External (not managed)" }
        2 { "External (managed)" }
        3 { "External (component)" }
        4 { "Unpacked (developer mode)" }
        5 { "Registry" }
        default { "Unknown ($($Found.location))" }
    }
    Write-Host "  Location: $Location" -ForegroundColor White
    
    if ($Found.install_time) {
        $InstallDate = [DateTimeOffset]::FromUnixTimeSeconds([long]$Found.install_time).LocalDateTime
        Write-Host "  Installed: $($InstallDate.ToString('yyyy-MM-dd HH:mm:ss'))" -ForegroundColor White
    }
    
    Write-Host ""
    
    # Check if path matches
    if ($Found.path -ne $ExtDir) {
        Write-Host "[WARN] Extension path doesn't match current folder!" -ForegroundColor Yellow
        Write-Host "       Registered: $($Found.path)" -ForegroundColor Gray
        Write-Host "       Current:    $ExtDir" -ForegroundColor Gray
        Write-Host ""
    }
    
    # Check if developer mode is enabled
    if ($Prefs.extensions.dev_mode) {
        Write-Host "Developer mode: Enabled ✅" -ForegroundColor Green
    } else {
        Write-Host "Developer mode: Disabled ⚠️" -ForegroundColor Yellow
    }
    
} else {
    Write-Host "Status: NOT INSTALLED ❌" -ForegroundColor Red
    Write-Host ""
    Write-Host "Extension not found in Edge Preferences." -ForegroundColor Gray
    Write-Host "Use the Extension button in the AI Secretary dashboard to install it." -ForegroundColor Yellow
}

Write-Host ""
