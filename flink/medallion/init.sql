-- Shared session setup for the medallion jobs (silver.sql, gold.sql).
-- Passed via `sql-client.sh -i`, so every job file sees the same catalog and
-- runtime settings.
CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'fluss-coordinator:9123'
);
USE CATALOG fluss_catalog;

SET 'execution.runtime-mode' = 'streaming';
-- Same reasoning as run-cdc-pipeline.sh: without a checkpoint interval,
-- sources and sinks that commit on checkpoint never make progress, and
-- nothing fails loudly.
SET 'execution.checkpointing.interval' = '10s';
SET 'parallelism.default' = '1';
-- Each INSERT ... statement set is submitted and the client returns,
-- instead of blocking on an unbounded streaming job.
SET 'table.dml-sync' = 'false';
