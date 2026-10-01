#!/bin/sh
# Renders flink/postgres-to-fluss.yaml with env vars (flink-cdc.sh has no
# built-in ${VAR} substitution) and submits it to the Flink standalone
# cluster via flink-cdc.sh. Meant to run from inside the flink-jobmanager
# container: `docker compose exec flink-jobmanager /opt/flink-cdc-pipelines/run-cdc-pipeline.sh`
set -e

SOURCE_PG_USER="${SOURCE_PG_USER:-ecommerce}"
SOURCE_PG_PASSWORD="${SOURCE_PG_PASSWORD:-ecommerce}"
SOURCE_PG_DB="${SOURCE_PG_DB:-ecommerce}"

RENDERED=/tmp/postgres-to-fluss.rendered.yaml
sed \
  -e "s/\${SOURCE_PG_USER}/${SOURCE_PG_USER}/g" \
  -e "s/\${SOURCE_PG_PASSWORD}/${SOURCE_PG_PASSWORD}/g" \
  -e "s/\${SOURCE_PG_DB}/${SOURCE_PG_DB}/g" \
  /opt/flink-cdc-pipelines/postgres-to-fluss.yaml > "$RENDERED"

echo "Submitting pipeline:"
cat "$RENDERED"
echo "---"
# execution.checkpointing.interval is required, not optional: Flink CDC's
# incremental snapshot framework (SnapshotSplitAssigner) only transitions
# the Postgres source from snapshot phase to streaming phase once a
# checkpoint completes after the last snapshot chunk finishes. Without a
# checkpoint interval configured, the job silently stays in "waiting for a
# complete checkpoint to mark the assigner finished" forever -- it snapshots
# existing rows fine, but never starts reading the replication slot for new
# changes. Confirmed by testing: with no interval set, an INSERT into
# Postgres after the pipeline started never reached Fluss.
"${FLINK_CDC_HOME}/bin/flink-cdc.sh" "$RENDERED" --flink-home /opt/flink \
  -D execution.checkpointing.interval=10s
