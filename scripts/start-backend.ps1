param(
  [string]$ApiBaseUrl,
  [string]$ApiKey,
  [string]$GenerationModel,
  [string]$ExplanationModel,
  [string]$TimeoutSeconds,
  [string]$SessionTtlSeconds,
  [Alias('Host')]
  [string]$BindHost,
  [string]$Port,
  [switch]$Help
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Show-Usage {
  @"
Usage:
  powershell -ExecutionPolicy Bypass -File scripts/start-backend.ps1 `
    -ApiBaseUrl URL `
    -ApiKey KEY `
    -GenerationModel MODEL `
    -ExplanationModel MODEL `
    -TimeoutSeconds SECONDS `
    -SessionTtlSeconds SECONDS `
    -Host HOST `
    -Port PORT

Required parameters:
  -ApiBaseUrl        LLM API base URL
  -ApiKey            LLM API key
  -GenerationModel   Model for pipeline generation
  -ExplanationModel  Model for explanation/review
  -TimeoutSeconds    Request timeout in seconds (integer)
  -SessionTtlSeconds Session TTL in seconds (integer)
  -Host              Backend bind host
  -Port              Backend bind port (integer)
"@
}

function Require-NonEmpty {
  param(
    [string]$Name,
    [string]$Value
  )

  if ([string]::IsNullOrWhiteSpace($Value)) {
    Write-Error "[ERROR] Missing required argument: $Name"
    Show-Usage
    exit 1
  }
}

function Require-Integer {
  param(
    [string]$Name,
    [string]$Value
  )

  if ($Value -notmatch '^[0-9]+$') {
    Write-Error "[ERROR] $Name must be an integer. Got: $Value"
    exit 1
  }
}

function Resolve-Python {
  $pythonCmd = Get-Command python -ErrorAction SilentlyContinue
  if ($pythonCmd) {
    return $pythonCmd.Name
  }

  $pyCmd = Get-Command py -ErrorAction SilentlyContinue
  if ($pyCmd) {
    return $pyCmd.Name
  }

  Write-Error "[ERROR] Python command not found. Install Python and ensure it is in PATH."
  exit 1
}

if ($Help) {
  Show-Usage
  exit 0
}

$rootDir = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$backendDir = Join-Path $rootDir 'src\backend'

if (-not (Test-Path -Path $backendDir -PathType Container)) {
  Write-Error "[ERROR] Backend directory not found: $backendDir"
  exit 1
}

Require-NonEmpty '-ApiBaseUrl' $ApiBaseUrl
Require-NonEmpty '-ApiKey' $ApiKey
Require-NonEmpty '-GenerationModel' $GenerationModel
Require-NonEmpty '-ExplanationModel' $ExplanationModel
Require-NonEmpty '-TimeoutSeconds' $TimeoutSeconds
Require-NonEmpty '-SessionTtlSeconds' $SessionTtlSeconds
Require-NonEmpty '-Host' $BindHost
Require-NonEmpty '-Port' $Port

Require-Integer '-TimeoutSeconds' $TimeoutSeconds
Require-Integer '-SessionTtlSeconds' $SessionTtlSeconds
Require-Integer '-Port' $Port

$env:ADP_API_BASE_URL = $ApiBaseUrl
$env:ADP_API_KEY = $ApiKey
$env:ADP_GENERATION_MODEL = $GenerationModel
$env:ADP_EXPLANATION_MODEL = $ExplanationModel
$env:ADP_TIMEOUT_SECONDS = $TimeoutSeconds
$env:ADP_SESSION_TTL_SECONDS = $SessionTtlSeconds
$env:PYTHONPATH = $backendDir

$pythonExe = Resolve-Python

Write-Host '[INFO] Installing backend dependencies...'
& $pythonExe -m pip install -r (Join-Path $backendDir 'requirements.txt')
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

Write-Host "[INFO] Starting backend at http://$BindHost`:$Port"
Push-Location $backendDir
try {
  & $pythonExe -m uvicorn app.main:app --reload --host $BindHost --port $Port
  if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
  }
}
finally {
  Pop-Location
}
