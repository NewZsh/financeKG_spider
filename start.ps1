# Windows PowerShell startup script for local development
# Run from the repository root with PowerShell:  .\start.ps1
# Ensure execution policy allows scripts (Set-ExecutionPolicy RemoteSigned).

Write-Host "Checking Python environment..."
try {
    & python --version > $null 2>&1
} catch {
    Write-Error "Python not found; please install or activate a virtual environment."
    exit 1
}

# Activate venv if present
if (Test-Path -Path '.\.venv\Scripts\Activate.ps1') {
    Write-Host "Activating .venv"
    . '.\.venv\Scripts\Activate.ps1'
} else {
    Write-Warning "No .venv found; using system Python environment."
}

# Install requirements file
if (Test-Path -Path '.\requirements.txt') {
    Write-Host "Installing Python dependencies..."
    pip install -r .\requirements.txt
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
$pythonCheck | python

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

Write-Host "Starting backend..."
Start-Process -NoNewWindow -FilePath python -ArgumentList "-m uvicorn web_app.backend.main:app --reload --port 8000" -PassThru | Out-Null

Write-Host "Starting frontend..."
Start-Process -NoNewWindow -FilePath "cmd.exe" -ArgumentList "/c npm run dev" -WorkingDirectory "web_app\frontend" -PassThru | Out-Null

Write-Host "Web application started: backend http://localhost:8000, frontend http://localhost:5173"
