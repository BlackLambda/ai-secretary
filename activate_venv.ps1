$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$activateScript = Join-Path $repoRoot ".venv\Scripts\Activate.ps1"

if (-not (Test-Path $activateScript)) {
    Write-Host "[ERROR] Virtual environment not found at $activateScript" -ForegroundColor Red
    Write-Host "Run .\start.bat first to create the Python 3.11 .venv." -ForegroundColor Yellow
    exit 1
}

. $activateScript
Write-Host "[INFO] Activated .venv for AI Secretary." -ForegroundColor Green
Write-Host "[INFO] Python: $(Get-Command python | Select-Object -ExpandProperty Source)" -ForegroundColor Gray