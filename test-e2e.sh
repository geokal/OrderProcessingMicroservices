#!/bin/bash
set -e

BASE_URL="http://localhost:5000"

echo "=========================================="
echo " Running E2E Microservices Test Suite "
echo "=========================================="

echo -n "1. Checking OrderService health... "
curl -s -f "$BASE_URL/health" > /dev/null && echo "[OK]" || (echo "[FAILED]" && exit 1)

echo "2. Submitting New Order (ord-e2e-1)..."
curl -i -X POST "$BASE_URL/orders" \
  -H "Idempotency-Key: e2e-key-1" \
  -H "Content-Type: application/json" \
  -d '{"OrderId": "ord-e2e-1", "Amount": 199.99}'

echo ""
echo "3. Testing Idempotency (Duplicate Submission)..."
curl -i -X POST "$BASE_URL/orders" \
  -H "Idempotency-Key: e2e-key-1" \
  -H "Content-Type: application/json" \
  -d '{"OrderId": "ord-e2e-1", "Amount": 199.99}'

echo ""
echo "4. Submitting Order with Transient Retry (retry-pass-2)..."
curl -i -X POST "$BASE_URL/orders" \
  -H "Idempotency-Key: e2e-key-2" \
  -H "Content-Type: application/json" \
  -d '{"OrderId": "retry-pass-2", "Amount": 150.00}'

echo ""
echo "5. Submitting Order targeting DLT (fail-dlt-3)..."
curl -i -X POST "$BASE_URL/orders" \
  -H "Idempotency-Key: e2e-key-3" \
  -H "Content-Type: application/json" \
  -d '{"OrderId": "fail-dlt-3", "Amount": 89.00}'

echo ""
echo "=========================================="
echo " E2E Test Suite Execution Complete! "
echo "=========================================="
