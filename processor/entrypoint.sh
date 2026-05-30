#!/bin/bash
set -e

CONNECT_URL="${CONNECT_URL:-http://connect:8083}"
MAX_RETRIES=60
SLEEP_SEC=10

echo "================================================"
echo " CDC Pipeline Processor Starting"
echo "================================================"

# ── Wait for Kafka Connect ──────────────────────────────────────────────────
echo "[entrypoint] Waiting for Kafka Connect at $CONNECT_URL ..."
for i in $(seq 1 $MAX_RETRIES); do
    HTTP=$(curl -s -o /dev/null -w "%{http_code}" "$CONNECT_URL/connectors" 2>/dev/null || echo "000")
    if [ "$HTTP" = "200" ]; then
        echo "[entrypoint] Kafka Connect is ready."
        break
    fi
    echo "[entrypoint] Attempt $i/$MAX_RETRIES - HTTP $HTTP. Waiting ${SLEEP_SEC}s ..."
    sleep $SLEEP_SEC
    if [ $i -eq $MAX_RETRIES ]; then
        echo "[entrypoint] WARNING: Kafka Connect did not become ready. Proceeding anyway ..."
    fi
done

# ── Register Debezium connector ──────────────────────────────────────────────
echo "[entrypoint] Registering Debezium connector ..."
CONNECTOR_FILE="/kafka-connect/connector.json"

if [ -f "$CONNECTOR_FILE" ]; then
    EXISTING=$(curl -s "$CONNECT_URL/connectors/mysql-cdc-connector" 2>/dev/null || echo "")
    if echo "$EXISTING" | grep -q '"name"'; then
        echo "[entrypoint] Connector already exists. Updating config ..."
        CONFIG=$(python3 -c "
import json, sys
with open('$CONNECTOR_FILE') as f:
    d = json.load(f)
print(json.dumps(d.get('config', d)))
")
        curl -s -X PUT \
            -H "Content-Type: application/json" \
            --data "$CONFIG" \
            "$CONNECT_URL/connectors/mysql-cdc-connector/config" || true
    else
        echo "[entrypoint] Registering new connector ..."
        curl -s -X POST \
            -H "Content-Type: application/json" \
            --data @"$CONNECTOR_FILE" \
            "$CONNECT_URL/connectors" || true
    fi
    echo "[entrypoint] Connector registration complete."
else
    echo "[entrypoint] WARNING: connector.json not found at $CONNECTOR_FILE"
fi

# Give connector time to start
sleep 5

# ── Start the processor ───────────────────────────────────────────────────────
echo "[entrypoint] Starting CDC processor ..."
exec python /app/processor.py
