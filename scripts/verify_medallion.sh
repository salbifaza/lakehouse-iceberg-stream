#!/usr/bin/env bash
# Verifies the streaming medallion: bronze -> silver -> gold, all tiered to
# Iceberg and read via Trino. (1) the three namespaces exist with the
# expected tables, (2) every gold table equals the same aggregation computed
# directly on source Postgres, (3) after live UPDATE / INSERT / DELETE /
# dimension-rename mutations, gold converges to Postgres again (retractions
# work end to end).
#
# Run after `make submit-pipeline`, `make submit-tiering`,
# `make submit-medallion`, and after the jobs report RUNNING (`make status`).
set -euo pipefail

COMPOSE="docker compose"
PG="$COMPOSE exec -T source-postgres psql -U ${SOURCE_PG_USER:-ecommerce} -d ${SOURCE_PG_DB:-ecommerce} -tA -F,"
TRINO="$COMPOSE exec -T trino trino --catalog lakehouse --output-format CSV_UNQUOTED --execute"
TIMEOUT="${MEDALLION_TIMEOUT:-480}"

# ── expected = computed on Postgres; actual = gold tables via Trino ─────────
# Column lists and ORDER BY must match pairwise. Integers/text/dates only.

pg_daily_revenue() { $PG -c "
  SELECT CAST(o.created_at AS DATE), p.category_id, c.name,
         COUNT(DISTINCT o.order_id), SUM(oi.quantity),
         SUM(oi.quantity::bigint * oi.unit_price_cents)
  FROM order_items oi
  JOIN orders o     ON o.order_id = oi.order_id
  JOIN products p   ON p.product_id = oi.product_id
  JOIN categories c ON c.category_id = p.category_id
  WHERE lower(trim(o.status)) <> 'cancelled'
  GROUP BY 1, 2, 3 ORDER BY 1, 2;"; }
trino_daily_revenue() { $TRINO "
  SELECT order_date, category_id, category_name, order_count, units_sold, revenue_cents
  FROM gold.daily_revenue_by_category ORDER BY 1, 2;"; }

pg_clv() { $PG -c "
  SELECT c.customer_id, lower(trim(c.email)), upper(trim(c.country)),
         COUNT(*), SUM(o.order_total_cents::bigint)
  FROM orders o JOIN customers c ON c.customer_id = o.customer_id
  WHERE lower(trim(o.status)) <> 'cancelled'
  GROUP BY 1, 2, 3 ORDER BY 1;"; }
trino_clv() { $TRINO "
  SELECT customer_id, email, country, order_count, lifetime_value_cents
  FROM gold.customer_lifetime_value ORDER BY 1;"; }

pg_status() { $PG -c "
  SELECT lower(trim(status)), COUNT(*), SUM(order_total_cents::bigint)
  FROM orders GROUP BY 1 ORDER BY 1;"; }
trino_status() { $TRINO "
  SELECT status, order_count, total_value_cents
  FROM gold.order_status_summary ORDER BY 1;"; }

# Prints one line per gold table; returns non-zero if any differ.
reconcile() {
  local rc=0
  for pair in daily_revenue:daily_revenue_by_category clv:customer_lifetime_value status:order_status_summary; do
    local fn="${pair%%:*}" table="${pair##*:}"
    if diff <(pg_$fn) <(trino_$fn) >/tmp/medallion_diff_$fn 2>&1; then
      printf "  %-28s OK (%s rows)\n" "$table" "$(pg_$fn | wc -l | tr -d ' ')"
    else
      printf "  %-28s MISMATCH\n" "$table"; rc=1
    fi
  done
  return $rc
}

wait_reconciled() {
  local deadline=$((SECONDS + TIMEOUT))
  while [ $SECONDS -lt $deadline ]; do
    reconcile >/tmp/medallion_last 2>&1 && { cat /tmp/medallion_last; return 0; }
    sleep 10
  done
  cat /tmp/medallion_last
  for f in /tmp/medallion_diff_*; do echo "--- $f (< postgres, > trino)"; cat "$f"; done
  return 1
}

echo "== Step 1: layers exist in Iceberg (via Trino) =="
for ns in bronze silver gold; do
  tables=$($TRINO "SHOW TABLES FROM ${ns};" | tr '\n' ' ')
  printf "  %-7s %s\n" "$ns" "$tables"
done
expected="bronze.categories bronze.customers bronze.orders bronze.order_items bronze.payments bronze.products
silver.customers silver.products silver.orders silver.order_lines silver.payments
gold.daily_revenue_by_category gold.customer_lifetime_value gold.order_status_summary"
for fq in $expected; do
  $TRINO "SHOW TABLES FROM ${fq%%.*} LIKE '${fq##*.}';" | grep -qx "${fq##*.}" \
    || { echo "  missing: $fq (not tiered yet, or medallion job not running?)" >&2; exit 1; }
done
echo "  all 14 tables present"

echo
echo "== Step 2: gold reconciles with Postgres (up to ${TIMEOUT}s) =="
wait_reconciled || { echo "Initial reconciliation FAILED." >&2; exit 1; }

echo
echo "== Step 3: live mutations -> gold converges again =="
echo "  toggling order_id=5 between 'paid' and 'cancelled'..."
$PG -c "UPDATE orders SET status = CASE WHEN status = 'cancelled' THEN 'paid' ELSE 'cancelled' END,
               updated_at = (now() AT TIME ZONE 'UTC') WHERE order_id = 5;" >/dev/null
echo "  toggling a ' (renamed)' suffix on category_id=3..."
$PG -c "UPDATE categories SET name = CASE WHEN name LIKE '% (renamed)' THEN left(name, -10)
                                          ELSE name || ' (renamed)' END WHERE category_id = 3;" >/dev/null
echo "  inserting a new order with 2 lines, then deleting one line..."
new_order=$($PG -c "INSERT INTO orders (customer_id, status, order_total_cents)
                    VALUES (4, 'paid', 13998) RETURNING order_id;" | head -1)
$PG -c "INSERT INTO order_items (order_id, product_id, quantity, unit_price_cents)
        VALUES (${new_order}, 1, 1, 9999), (${new_order}, 10, 1, 3999);" >/dev/null
$PG -c "DELETE FROM order_items WHERE order_id = ${new_order} AND product_id = 10;" >/dev/null

wait_reconciled || { echo "Medallion verification FAILED -- gold did not converge within ${TIMEOUT}s." >&2; exit 1; }

echo
echo "Medallion verification PASSED."
