# Windows PowerShell startup script for local development
# Run from the repository root with PowerShell:  .\start.ps1
# Ensure execution policy allows scripts (Set-ExecutionPolicy RemoteSigned).

Set-StrictMode -Version Latest

Write-Host "Checking Python environment..."

$projectVenv = (Resolve-Path '.\.venv').Path
$projectPython = Join-Path $projectVenv 'Scripts\python.exe'

if (-not (Test-Path -Path $projectPython)) {
    Write-Error "Project virtual environment is missing. Expected: $projectPython"
    exit 1
}

if ($env:VIRTUAL_ENV -and ((Resolve-Path $env:VIRTUAL_ENV).Path -eq $projectVenv)) {
    Write-Host "Using active virtual environment: $env:VIRTUAL_ENV"
} elseif (Test-Path -Path '.\.venv\Scripts\Activate.ps1') {
    Write-Host "Activating .venv"
    . '.\.venv\Scripts\Activate.ps1'
} else {
    Write-Warning "No project virtual environment found; using current Python environment."
}

try {
    & $projectPython --version > $null 2>&1
} catch {
    Write-Error "Python not found; please install or activate a virtual environment."
    exit 1
}

$pythonVersionOutput = & $projectPython -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
$pythonVersion = [version]$pythonVersionOutput
if ($pythonVersion -lt [version]'3.9') {
    Write-Error "Python 3.9+ is required. Current version: $pythonVersion. Please recreate .venv with a newer Python."
    exit 1
}

Write-Host "Using Python $pythonVersion"

# Install requirements file
if (Test-Path -Path '.\requirements.txt') {
    Write-Host "Installing Python dependencies..."

    if (Get-Command uv -ErrorAction SilentlyContinue) {
        Write-Host "Installing dependencies with uv pip"
        uv pip install --python $projectPython -r .\requirements.txt
    } else {
        Write-Host "uv not found; falling back to python -m pip"
        & $projectPython -m pip install -r .\requirements.txt
    }
} else {
    Write-Error "requirements.txt not found; run script from project root."
    exit 1
}

# Optional explicit check for a few packages
$pythonCheck = @"
import importlib,sys
for pkg in ('fastapi','uvicorn','pydantic'):
    try:
        importlib.import_module(pkg)
    except ImportError:
        print(f"Missing package {pkg}.", file=sys.stderr)
        sys.exit(1)
print('Python package check passed.')
"@
$pythonCheck | & $projectPython

# Check Node.js/npm
Write-Host "Checking Node.js/npm..."
if (-not (Get-Command npm -ErrorAction SilentlyContinue)) {
    Write-Error "npm not found; please install Node.js."
    exit 1
}

# Verify Neo4j listening ports 7474 (HTTP) and 7687 (Bolt)
$neo4jHTTP = Test-NetConnection -ComputerName 127.0.0.1 -Port 7474
$neo4jBolt = Test-NetConnection -ComputerName 127.0.0.1 -Port 7687
if (-not ($neo4jHTTP.TcpTestSucceeded -and $neo4jBolt.TcpTestSucceeded)) {
    Write-Error "Neo4j not reachable; start database so ports 7474 and 7687 are open."
    exit 1
}

# Start backend and frontend
Push-Location 'web_app\frontend'
npm install
Pop-Location

function Stop-ProjectProcessOnPort {
    param(
        [int]$Port
    )

    $connections = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
    foreach ($connection in $connections) {
        $process = Get-CimInstance Win32_Process -Filter "ProcessId = $($connection.OwningProcess)"
        if ($null -eq $process) {
            continue
        }

        $commandLine = $process.CommandLine
        if ($commandLine -and $commandLine.Contains('financeKG_spider')) {
            Write-Host "Stopping existing project process on port $Port (PID $($process.ProcessId))"
            Stop-Process -Id $process.ProcessId -Force
        } else {
            Write-Error "Port $Port is already in use by PID $($process.ProcessId). Resolve it before starting the app."
            exit 1
        }
    }
}

Stop-ProjectProcessOnPort -Port 8000
Stop-ProjectProcessOnPort -Port 5173

Write-Host "Starting backend..."
Start-Process -NoNewWindow -WorkingDirectory (Resolve-Path '.').Path -FilePath $projectPython -ArgumentList '-m','uvicorn','web_app.backend.main:app','--reload','--host','127.0.0.1','--port','8000' -PassThru | Out-Null

Write-Host "Starting frontend..."
Start-Process -NoNewWindow -WorkingDirectory (Resolve-Path 'web_app\frontend').Path -FilePath 'npm.cmd' -ArgumentList 'run','dev','--','--host','127.0.0.1','--port','5173','--strictPort' -PassThru | Out-Null

Write-Host "Web application started: backend http://localhost:8000, frontend http://localhost:5173"
