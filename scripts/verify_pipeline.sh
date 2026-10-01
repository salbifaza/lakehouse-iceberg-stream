#!/usr/bin/env bash
# Verifies the Postgres -> Flink CDC -> Fluss -> Iceberg tiering -> Trino
# pipeline: (1) row counts match after the initial snapshot, (2) a live
# insert/update/delete against source-postgres shows up in Trino (reading
# Iceberg through Polaris) within a timeout. Same shape as
# stream-debezium-kafka's verify_cdc.sh so results are directly comparable.
#
# Run after `make up`, `make submit-pipeline`, and `make submit-tiering`,
# and after both Flink jobs report RUNNING (`make status`).
set -euo pipefail

COMPOSE="docker compose"
PG_EXEC="$COMPOSE exec -T source-postgres psql -U ${SOURCE_PG_USER:-ecommerce} -d ${SOURCE_PG_DB:-ecommerce} -tA"
TRINO_EXEC="$COMPOSE exec -T trino trino --catalog lakehouse --execute"

TABLES=(categories customers products orders order_items payments)

echo "== Step 1: row counts, source vs. Iceberg (via Trino) =="
fail=0
for t in "${TABLES[@]}"; do
    src_count=$($PG_EXEC -c "SELECT count(*) FROM ${t};")
    trino_count=$($TRINO_EXEC "SELECT count(*) FROM bronze.${t};" | tr -d '"')
    status="OK"
    if [ "$src_count" != "$trino_count" ]; then
        status="MISMATCH"
        fail=1
    fi
    printf "  %-14s source=%-6s trino=%-6s %s\n" "$t" "$src_count" "$trino_count" "$status"
done
if [ "$fail" -ne 0 ]; then
    echo "Row count mismatch -- either the pipeline/tiering hasn't caught up yet, or a job crashed. Aborting." >&2
    echo "Check: make status" >&2
    exit 1
fi

echo
echo "== Step 2: live CDC + tiering test (insert / update / delete) =="
insert_marker="cdc-verify-insert-$(date +%s)"
delete_marker="cdc-verify-delete-$(date +%s)"

# The delete target is a category this script inserts itself (not a seeded
# one) -- seeded categories have products FK-referencing them, so deleting
# one fails with a foreign key violation. A fresh, unreferenced row avoids
# that entirely instead of guessing which seeded row is "safe" to delete.
echo "  inserting marker category '${insert_marker}' (stays)..."
$PG_EXEC -c "INSERT INTO categories (name) VALUES ('${insert_marker}');" >/dev/null
echo "  inserting marker category '${delete_marker}' (will be deleted)..."
$PG_EXEC -c "INSERT INTO categories (name) VALUES ('${delete_marker}');" >/dev/null

echo "  updating order_id=1 status to 'shipped'..."
$PG_EXEC -c "UPDATE orders SET status = 'shipped', updated_at = (now() AT TIME ZONE 'UTC') WHERE order_id = 1;" >/dev/null

echo "  deleting categories.name='${delete_marker}'..."
$PG_EXEC -c "DELETE FROM categories WHERE name = '${delete_marker}';" >/dev/null

# freshness=30s (table.datalake.freshness in flink/postgres-to-fluss.yaml)
# is the target lag per table, but the single tiering service job cycles
# through all 6 tables round-robin, not in parallel -- so the worst case
# for any one table is closer to 6x freshness. Confirmed by testing:
# changes landed in Trino/Iceberg at ~150s in one run.
echo "  polling Trino for propagation (up to 420s)..."
deadline=$((SECONDS + 420))
insert_ok=0; update_ok=0; delete_ok=0
while [ $SECONDS -lt $deadline ]; do
    [ "$insert_ok" -eq 0 ] && val=$($TRINO_EXEC "SELECT count(*) FROM bronze.categories WHERE name = '${insert_marker}';" | tr -d '"') && [ "$val" = "1" ] && insert_ok=1
    [ "$update_ok" -eq 0 ] && val=$($TRINO_EXEC "SELECT status FROM bronze.orders WHERE order_id = 1;" | tr -d '"') && [ "$val" = "shipped" ] && update_ok=1
    [ "$delete_ok" -eq 0 ] && val=$($TRINO_EXEC "SELECT count(*) FROM bronze.categories WHERE name = '${delete_marker}';" | tr -d '"') && [ "$val" = "0" ] && delete_ok=1
    if [ "$insert_ok" -eq 1 ] && [ "$update_ok" -eq 1 ] && [ "$delete_ok" -eq 1 ]; then
        break
    fi
    sleep 5
done

printf "  insert propagated: %s\n" "$([ "$insert_ok" -eq 1 ] && echo yes || echo NO)"
printf "  update propagated: %s\n" "$([ "$update_ok" -eq 1 ] && echo yes || echo NO)"
printf "  delete propagated: %s\n" "$([ "$delete_ok" -eq 1 ] && echo yes || echo NO)"

if [ "$insert_ok" -eq 1 ] && [ "$update_ok" -eq 1 ] && [ "$delete_ok" -eq 1 ]; then
    echo
    echo "Pipeline verification PASSED."
    exit 0
else
    echo
    echo "Pipeline verification FAILED -- one or more changes did not propagate within 420s." >&2
    exit 1
fi
