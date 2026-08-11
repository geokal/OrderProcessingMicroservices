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
  -H "Idempotency-Key: demo-dlt-key" \
  -H "Content-Type: application/json" \
  -d '{"OrderId": "fail-dlt-3", "Amount": 89.00}'

echo ""
echo "6. Testing Rate Limiting (burst of 120 requests)..."
echo "  Sending 120 rapid requests to verify 429 responses..."
SUCCESS=0
RATE_LIMITED=0
for i in $(seq 1 120); do
  CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE_URL/orders" \
    -H "Idempotency-Key: rate-test-$i" \
    -H "Content-Type: application/json" \
    -d "{\"OrderId\": \"rate-test-$i\", \"Amount\": 49.99}")
  if [ "$CODE" = "202" ]; then
    SUCCESS=$((SUCCESS+1))
  elif [ "$CODE" = "429" ]; then
    RATE_LIMITED=$((RATE_LIMITED+1))
  fi
done
echo "  Results: $SUCCESS succeeded, $RATE_LIMITED rate-limited (429)"

echo "7. Simulating Debezium restart..."
docker-compose restart debezium
echo "  Waiting 20 seconds for connector recovery..."
sleep 20
curl -sf http://localhost:8083/connectors/orders-outbox-connector/status > /dev/null && echo "[OK] Connector recovered" || echo "[WARN] Connector may need re-registration"

echo ""
echo "=========================================="
echo " E2E Test Suite Execution Complete! "
echo "=========================================="
