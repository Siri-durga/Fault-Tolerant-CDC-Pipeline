#!/bin/bash
# Register Debezium MySQL connector once Kafka Connect is ready

CONNECT_URL="${CONNECT_URL:-http://connect:8083}"
CONNECTOR_FILE="/kafka-connect/connector.json"
MAX_RETRIES=30
RETRY_INTERVAL=10

echo "==> Waiting for Kafka Connect to be ready at $CONNECT_URL ..."
for i in $(seq 1 $MAX_RETRIES); do
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$CONNECT_URL/connectors")
    if [ "$HTTP_CODE" = "200" ]; then
        echo "==> Kafka Connect is ready."
        break
    fi
    echo "    Attempt $i/$MAX_RETRIES - HTTP $HTTP_CODE. Retrying in ${RETRY_INTERVAL}s ..."
    sleep $RETRY_INTERVAL
done

# Check if connector already exists
EXISTING=$(curl -s "$CONNECT_URL/connectors/mysql-cdc-connector")
if echo "$EXISTING" | grep -q '"name"'; then
    echo "==> Connector already registered. Updating..."
    curl -s -X PUT \
        -H "Content-Type: application/json" \
        --data @<(cat "$CONNECTOR_FILE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(json.dumps(d['config']))") \
        "$CONNECT_URL/connectors/mysql-cdc-connector/config"
else
    echo "==> Registering new connector..."
    curl -s -X POST \
        -H "Content-Type: application/json" \
        --data @"$CONNECTOR_FILE" \
        "$CONNECT_URL/connectors"
fi

echo ""
echo "==> Connector status:"
sleep 3
curl -s "$CONNECT_URL/connectors/mysql-cdc-connector/status" | python3 -m json.tool 2>/dev/null || echo "Status not available yet."
