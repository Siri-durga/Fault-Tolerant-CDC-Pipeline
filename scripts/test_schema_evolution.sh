#!/bin/bash
# ============================================================
# test_schema_evolution.sh
# Run this AFTER docker-compose up to test schema evolution
# ============================================================
set -e

MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-debezium}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-dbzpassword}"
MYSQL_DATABASE="${MYSQL_DATABASE:-ecommerce}"

run_sql() {
    docker exec cdc-mysql mysql -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "$1" 2>/dev/null
}

echo "================================================"
echo " CDC Schema Evolution Test"
echo "================================================"
echo ""

# ── Test 1: Column rename on products ────────────────────────────────────────
echo "[Test 1] Renaming products.description → product_description ..."
run_sql "ALTER TABLE products RENAME COLUMN description TO product_description;"
echo "  Done."

echo "[Test 1] Inserting a new product after column rename ..."
run_sql "INSERT INTO products (name, product_description, price, stock, category, sku, weight_kg)
         VALUES ('Schema Test Product', 'Inserted after rename', 99.99, 50, 'Test', 'SKU-TEST-001', 0.500);"
echo "  Done. Waiting 15s for CDC to process ..."
sleep 15
echo ""

# ── Test 2: Add NOT NULL column with default to customers ─────────────────────
echo "[Test 2] Adding country_code column to customers ..."
run_sql "ALTER TABLE customers ADD COLUMN country_code VARCHAR(3) NOT NULL DEFAULT 'USA';" 2>/dev/null || \
    echo "  (column may already exist)"
echo "  Done."

echo "[Test 2] Inserting a new customer after column addition ..."
run_sql "INSERT INTO customers (first_name, last_name, email, phone, address, city, state, zip_code)
         VALUES ('Test', 'User', 'test.user.evolution@example.com', '555-0000-0000', '123 Test St', 'Testville', 'TX', '00000');"
echo "  Done. Waiting 15s for CDC to process ..."
sleep 15
echo ""

# ── Test 3: Update on customers ───────────────────────────────────────────────
echo "[Test 3] Updating a customer record ..."
run_sql "UPDATE customers SET city='UpdatedCity' WHERE email='test.user.evolution@example.com';"
echo "  Done. Waiting 10s ..."
sleep 10
echo ""

echo "================================================"
echo " Schema Evolution Test Complete!"
echo " Check ./data_lake/ for Parquet output files."
echo " Check ./state/schemas.db for schema versions."
echo " Check ./output/lineage_report.json for lineage."
echo "================================================"
