param(
    [int]$InitialiseCount = 0
)

$ErrorActionPreference = "Stop"

Write-Host "[1/4] Checking Docker availability..."
docker --version | Out-Null
docker-compose version | Out-Null

Write-Host "[2/4] Starting containers..."
docker-compose up -d

Write-Host "[3/4] Verifying service status..."
docker-compose ps

if ($InitialiseCount -gt 0) {
    Write-Host "[4/4] Triggering pipeline initialise with count=$InitialiseCount ..."
    $url = "http://127.0.0.1:8080/api/pipeline/initialise?count=$InitialiseCount"
    try {
        Invoke-RestMethod -Method POST -Uri $url | Out-Null
        Write-Host "Pipeline initialise request submitted."
    }
    catch {
        Write-Host "Dashboard may not be running yet. Start it with: python dashboard/run.py"
        Write-Host "Then rerun initialise manually using: POST $url"
    }
}

Write-Host "Done. Start dashboard with: python dashboard/run.py"
Write-Host "Open: http://127.0.0.1:8080/"
