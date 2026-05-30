#!/bin/bash
# ============================================================
# verify.sh
# Verifies all 8 contract requirements are met
# ============================================================

MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-debezium}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-dbzpassword}"
MYSQL_DATABASE="${MYSQL_DATABASE:-ecommerce}"
TODAY=$(date +%Y-%m-%d)

PASS=0
FAIL=0

check() {
    local desc="$1"
    local result="$2"
    if [ "$result" = "0" ]; then
        echo "  ✅ PASS: $desc"
        PASS=$((PASS+1))
    else
        echo "  ❌ FAIL: $desc"
        FAIL=$((FAIL+1))
    fi
}

run_sql() {
    docker exec cdc-mysql mysql -u "$MYSQL_USER" \
          -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -sNe "$1" 2>/dev/null
}

echo "================================================"
echo " CDC Pipeline Verification"
echo " Date: $TODAY"
echo "================================================"
echo ""

# ── Req 1: docker-compose.yml ────────────────────────────────────────────────
echo "[Req 1] Docker Compose file and services ..."
[ -f "docker-compose.yml" ]; check "docker-compose.yml exists" $?
grep -q "mysql:" docker-compose.yml; check "mysql service defined" $?
grep -q "kafka:" docker-compose.yml; check "kafka service defined" $?
grep -q "zookeeper:" docker-compose.yml; check "zookeeper service defined" $?
grep -q "connect:" docker-compose.yml; check "connect service defined" $?
grep -q "processor:" docker-compose.yml; check "processor service defined" $?
grep -q "healthcheck:" docker-compose.yml; check "healthcheck present" $?
echo ""

# ── Req 2: MySQL tables and row count ────────────────────────────────────────
echo "[Req 2] MySQL tables and data volume ..."
TABLES=$(run_sql "SHOW TABLES;" 2>/dev/null)
echo "$TABLES" | grep -q "customers"; check "customers table exists" $?
echo "$TABLES" | grep -q "products"; check "products table exists" $?
echo "$TABLES" | grep -q "orders"; check "orders table exists" $?
C=$(run_sql "SELECT COUNT(*) FROM customers;" 2>/dev/null || echo 0)
P=$(run_sql "SELECT COUNT(*) FROM products;" 2>/dev/null || echo 0)
O=$(run_sql "SELECT COUNT(*) FROM orders;" 2>/dev/null || echo 0)
TOTAL=$((C + P + O))
echo "  Customers: $C  Products: $P  Orders: $O  Total: $TOTAL"
[ "$TOTAL" -gt 500000 ]; check "Total row count > 500,000" $?
echo ""

# ── Req 3: Parquet output in data_lake ───────────────────────────────────────
echo "[Req 3] Parquet output files ..."
[ -d "data_lake" ]; check "data_lake directory exists" $?
PARQUET_COUNT=$(find data_lake -name "*.parquet" 2>/dev/null | wc -l)
echo "  Found $PARQUET_COUNT parquet files"
[ "$PARQUET_COUNT" -gt 0 ]; check "Parquet files exist in data_lake" $?
# Check structure: data_lake/<table>/<date>/<op>/
STRUCT=$(find data_lake -mindepth 3 -maxdepth 3 -type d 2>/dev/null | head -1)
[ -n "$STRUCT" ]; check "data_lake partition structure exists" $?
echo ""

# ── Req 4: Schema store ───────────────────────────────────────────────────────
echo "[Req 4] Schema store ..."
[ -f "state/schemas.db" ]; check "state/schemas.db exists" $?
if [ -f "state/schemas.db" ]; then
    SCHEMA_COUNT=$(python3 -c "import sqlite3; print(sqlite3.connect('state/schemas.db').execute('SELECT COUNT(*) FROM schema_versions').fetchone()[0])" 2>/dev/null || echo 0)
    echo "  Schema versions recorded: $SCHEMA_COUNT"
    [ "$SCHEMA_COUNT" -gt 0 ]; check "Schema versions present in store" $?
fi
echo ""

# ── Req 5 & 6: Schema evolution ──────────────────────────────────────────────
echo "[Req 5] Column rename (description → product_description) ..."
COLS=$(run_sql "SHOW COLUMNS FROM products;" 2>/dev/null)
echo "$COLS" | grep -q "product_description"; check "product_description column exists" $?
! echo "$COLS" | grep -q "^description"; check "old description column removed" $?
echo ""

echo "[Req 6] New NOT NULL column (country_code) ..."
CUST_COLS=$(run_sql "SHOW COLUMNS FROM customers;" 2>/dev/null)
echo "$CUST_COLS" | grep -q "country_code"; check "country_code column exists" $?
echo ""

# ── Req 7: Lineage report ─────────────────────────────────────────────────────
echo "[Req 7] Lineage report ..."
[ -f "output/lineage_report.json" ]; check "output/lineage_report.json exists" $?
if [ -f "output/lineage_report.json" ]; then
    python3 -c "
import json, sys
with open('output/lineage_report.json') as f:
    data = json.load(f)
assert isinstance(data, list), 'Not a list'
for entry in data:
    assert 'source_table' in entry
    assert 'schema_version' in entry
    assert 'active_from' in entry
    assert 'output_partitions' in entry
    assert 'output_schema' in entry
print(f'  Valid JSON array with {len(data)} entries')
" 2>/dev/null
    check "Lineage report valid JSON structure" $?
fi
echo ""

# ── Req 8: .env.example ──────────────────────────────────────────────────────
echo "[Req 8] .env.example ..."
[ -f ".env.example" ]; check ".env.example exists" $?
grep -q "MYSQL_USER" .env.example; check "MYSQL_USER in .env.example" $?
grep -q "MYSQL_PASSWORD" .env.example; check "MYSQL_PASSWORD in .env.example" $?
grep -q "MYSQL_DATABASE" .env.example; check "MYSQL_DATABASE in .env.example" $?
grep -q "KAFKA_BOOTSTRAP" .env.example; check "KAFKA_BOOTSTRAP_SERVERS in .env.example" $?
echo ""

echo "================================================"
echo " Results: $PASS passed, $FAIL failed"
echo "================================================"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
