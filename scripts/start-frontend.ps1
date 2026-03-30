param(
  [string]$ApiBaseUrl,
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
  powershell -ExecutionPolicy Bypass -File scripts/start-frontend.ps1 `
    -ApiBaseUrl URL `
    -Host HOST `
    -Port PORT

Required parameters:
  -ApiBaseUrl Frontend API base URL (NEXT_PUBLIC_API_BASE_URL)
  -Host       Frontend bind host
  -Port       Frontend bind port (integer)
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

function Resolve-Npm {
  $npmCmd = Get-Command npm -ErrorAction SilentlyContinue
  if ($npmCmd) {
    return $npmCmd.Name
  }

  Write-Error '[ERROR] npm command not found. Install Node.js and ensure npm is in PATH.'
  exit 1
}

if ($Help) {
  Show-Usage
  exit 0
}

$rootDir = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$frontendDir = Join-Path $rootDir 'src\frontend'

if (-not (Test-Path -Path $frontendDir -PathType Container)) {
  Write-Error "[ERROR] Frontend directory not found: $frontendDir"
  exit 1
}

Require-NonEmpty '-ApiBaseUrl' $ApiBaseUrl
Require-NonEmpty '-Host' $BindHost
Require-NonEmpty '-Port' $Port

Require-Integer '-Port' $Port

$env:NEXT_PUBLIC_API_BASE_URL = $ApiBaseUrl

$npmExe = Resolve-Npm

Write-Host '[INFO] Installing frontend dependencies...'
Push-Location $frontendDir
try {
  & $npmExe install
  if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
  }

  Write-Host "[INFO] Starting frontend at http://$BindHost`:$Port"
  & $npmExe run dev -- --hostname $BindHost --port $Port
  if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
  }
}
finally {
  Pop-Location
}
