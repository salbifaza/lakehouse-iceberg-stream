#!/bin/sh
# Submits the medallion jobs (bronze -> silver, then silver -> gold) via the
# Flink SQL client. Run from inside flink-jobmanager:
#   docker compose exec flink-jobmanager /opt/flink-cdc-pipelines/run-medallion.sh
#
# Ordering matters: the silver job can't be planned until the CDC pipeline
# has created all six bronze tables in Fluss, and gold can't be planned
# until the silver tables exist. So: wait for bronze, create silver tables,
# submit silver, create gold tables, submit gold.
set -e

DIR=/opt/flink-cdc-pipelines/medallion
SQL_CLIENT=/opt/flink/bin/sql-client.sh
BRONZE_TABLES="categories customers products orders order_items payments"
SILVER_TABLES="customers products orders order_lines payments"
GOLD_TABLES="daily_revenue_by_category customer_lifetime_value order_status_summary"

# sql-client.sh -f exits 0 even when a statement fails (it prints [ERROR]
# and stops executing the file), so detect failure from its output.
run_sql() {
  log=/tmp/medallion-$(basename "$1" .sql).log
  "$SQL_CLIENT" -i "$DIR/init.sql" -f "$1" > "$log" 2>&1 || true
  if grep -q "\[ERROR\]" "$log"; then
    grep -A3 "\[ERROR\]" "$log" >&2
    echo "Failed: $1 (full log: $log in flink-jobmanager)" >&2
    exit 1
  fi
  grep -E "Job ID" "$log" || true
}

# Prints the expected tables (from $2) missing from Fluss database $1.
missing_tables() {
  probe=/tmp/medallion-probe.sql
  echo "SHOW TABLES FROM $1;" > "$probe"
  out=$("$SQL_CLIENT" -i "$DIR/init.sql" -f "$probe" 2>/dev/null || true)
  for t in $2; do
    echo "$out" | grep -qw "$t" || printf '%s ' "$t"
  done
}

# Creates a layer's tables only if none exist yet. With table.datalake.enabled,
# Fluss 0.9.1 rejects CREATE TABLE IF NOT EXISTS once the tiered Iceberg table
# exists, so the DDL file can't simply be re-run on every submit.
ensure_tables() {
  missing=$(missing_tables "$1" "$2")
  if [ -z "$missing" ]; then
    echo "All $1 tables already exist."
  elif [ "$(echo $missing | wc -w)" -eq "$(echo $2 | wc -w)" ]; then
    echo "Creating $1 tables..."
    run_sql "$DIR/$1_tables.sql"
  else
    echo "Partial $1 layer (missing: $missing) -- create those tables from" >&2
    echo "$DIR/$1_tables.sql by hand, then rerun." >&2
    exit 1
  fi
}

echo "Waiting for bronze tables (created by the CDC pipeline)..."
i=0
while [ -n "$(missing_tables bronze "$BRONZE_TABLES")" ]; do
  i=$((i + 1))
  if [ "$i" -ge 30 ]; then
    echo "Bronze tables still missing after ~5 min: $(missing_tables bronze "$BRONZE_TABLES")" >&2
    echo "Is the CDC pipeline running? (make status)" >&2
    exit 1
  fi
  sleep 10
done
echo "All bronze tables present."

ensure_tables silver "$SILVER_TABLES"
echo "Submitting bronze -> silver..."
run_sql "$DIR/silver_job.sql"

ensure_tables gold "$GOLD_TABLES"
echo "Submitting silver -> gold..."
run_sql "$DIR/gold_job.sql"
