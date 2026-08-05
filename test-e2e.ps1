# End-to-End Automated Test Script for Order Processing Microservices

$baseUrl = "http://localhost:5000"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host " Running E2E Microservices Test Suite " -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

# 1. Health Check
Write-Host "`n1. Testing OrderService Health Endpoint..." -NoNewline
try {
    $health = Invoke-RestMethod -Uri "$baseUrl/health" -Method Get
    Write-Host " [OK]" -ForegroundColor Green
} catch {
    Write-Host " [FAILED]" -ForegroundColor Red
    Write-Host "Error connecting to $baseUrl/health: $_"
    exit 1
}

# 2. Submit New Order
Write-Host "`n2. Submitting New Order (OrderId: ord-e2e-100, Key: e2e-key-100)..."
$headers = @{
    "Idempotency-Key" = "e2e-key-100"
    "Content-Type"    = "application/json"
}
$body = @{
    OrderId = "ord-e2e-100"
    Amount  = 299.99
} | ConvertTo-Json

try {
    $response = Invoke-WebRequest -Uri "$baseUrl/orders" -Method Post -Headers $headers -Body $body -UseBasicParsing
    Write-Host " Status Code: $($response.StatusCode)" -ForegroundColor Green
    Write-Host " Response: $($response.Content)" -ForegroundColor Yellow
} catch {
    Write-Host " [FAILED] $_" -ForegroundColor Red
}

# 3. Test Idempotency (Duplicate Submission)
Write-Host "`n3. Submitting Duplicate Order (Idempotency Check)..."
try {
    $dupResponse = Invoke-WebRequest -Uri "$baseUrl/orders" -Method Post -Headers $headers -Body $body -UseBasicParsing
    Write-Host " Status Code: $($dupResponse.StatusCode)" -ForegroundColor Green
    Write-Host " Response: $($dupResponse.Content)" -ForegroundColor Yellow
    if ($dupResponse.Content -like "*idempotently*") {
        Write-Host " -> Idempotency Verified Successfully!" -ForegroundColor Green
    }
} catch {
    Write-Host " [FAILED] $_" -ForegroundColor Red
}

# 4. Test Transient Retry Order (retry-pass-101)
Write-Host "`n4. Submitting Order with Transient Retry (OrderId: retry-pass-101)..."
$retryHeaders = @{
    "Idempotency-Key" = "e2e-key-101"
    "Content-Type"    = "application/json"
}
$retryBody = @{
    OrderId = "retry-pass-101"
    Amount  = 150.00
} | ConvertTo-Json

try {
    $retryRes = Invoke-WebRequest -Uri "$baseUrl/orders" -Method Post -Headers $retryHeaders -Body $retryBody -UseBasicParsing
    Write-Host " Status Code: $($retryRes.StatusCode)" -ForegroundColor Green
    Write-Host " Response: $($retryRes.Content)" -ForegroundColor Yellow
} catch {
    Write-Host " [FAILED] $_" -ForegroundColor Red
}

# 5. Test DLT Order (fail-dlt-102)
Write-Host "`n5. Submitting Order targeting DLT (OrderId: fail-dlt-102)..."
$dltHeaders = @{
    "Idempotency-Key" = "e2e-key-102"
    "Content-Type"    = "application/json"
}
$dltBody = @{
    OrderId = "fail-dlt-102"
    Amount  = 89.00
} | ConvertTo-Json

try {
    $dltRes = Invoke-WebRequest -Uri "$baseUrl/orders" -Method Post -Headers $dltHeaders -Body $dltBody -UseBasicParsing
    Write-Host " Status Code: $($dltRes.StatusCode)" -ForegroundColor Green
    Write-Host " Response: $($dltRes.Content)" -ForegroundColor Yellow
} catch {
    Write-Host " [FAILED] $_" -ForegroundColor Red
}

Write-Host "`n==========================================" -ForegroundColor Cyan
Write-Host " E2E Test Execution Completed! " -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
