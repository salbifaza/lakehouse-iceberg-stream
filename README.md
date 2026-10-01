# Postgres → Apache Fluss → Apache Iceberg, on SeaweedFS

A working, end-to-end streaming lakehouse: [Apache Fluss](https://fluss.apache.org/)
replaces Kafka + Debezium + Kafka Connect as the CDC middle tier, and
[SeaweedFS](https://github.com/seaweedfs/seaweedfs) replaces MinIO as the
object store underneath the lakehouse. Same e-commerce source schema and seed
data as [`stream-debezium-kafka`](https://github.com/salbifaza/stream-debezium-kafka) and
[`stream-cdc-peerdb`](https://github.com/salbifaza/stream-cdc-peerdb), so this is a genuine third data
point in that comparison, not a different demo.

**This is not a "Fluss vs. Kafka" writeup for its own sake.** It's a
from-scratch build, verified end to end with `make verify`, of the specific
claim Fluss makes about itself: that it can collapse a Kafka+Debezium+lakehouse
pipeline into one system. It does — but five things had to be worked around
or fixed to get there, and they're the most useful part of this repo.

## Contents

[Architecture](#architecture) ·
[Streaming medallion](#streaming-medallion) ·
[Quickstart](#quickstart) ·
[What this proves](#what-this-actually-proves) ·
[Findings — what broke and why](#findings--what-broke-and-why) ·
[Comparison to stream-debezium-kafka](#comparison-to-stream-debezium-kafka) ·
[Repo layout](#repo-layout) ·
[Production considerations](#production-considerations)

## Architecture

```
Postgres (source, logical replication)
    │  Flink CDC (postgres-cdc pipeline connector, route public.* -> bronze.*)
    ▼
Fluss bronze.*  ──Flink SQL──▶  Fluss silver.*  ──Flink SQL──▶  Fluss gold.*
 (current state)    (clean / conform / join)      (retraction-safe aggregates)
    │                         │                              │
    └──────── Fluss datalake tiering service (2 Flink jobs) ─┘
                              ▼
     Iceberg namespaces bronze / silver / gold (Apache Polaris REST catalog)
                              │  Parquet + Avro + JSON metadata
                              ▼
     SeaweedFS (S3-compatible object storage, replaces MinIO)
                              ▼
     Trino (query engine, reads Iceberg through Polaris)
```

Fluss is the hot tier (PK tables, replaces Kafka+Debezium+Kafka Connect) for
every layer; Iceberg is the cold tier for every layer.

Services (`docker-compose.yml`):

| Service | Role |
|---|---|
| `source-postgres` | CDC source, same schema/seed data as sibling repos |
| `flink-jobmanager` / `flink-taskmanager` | Runs the CDC pipeline job, 2 tiering service jobs, and the silver + gold medallion jobs (8 slots, 6 used) |
| `fluss-coordinator` / `fluss-tablet` | The Fluss cluster (hot tier) |
| `zookeeper` | Fluss's metadata store (Fluss has no KRaft-equivalent yet) |
| `seaweedfs` | S3-compatible object storage (`weed mini`: master+volume+filer+S3 gateway in one process) |
| `polaris` / `polaris-postgres` | Iceberg REST catalog + its metastore |
| `trino` | Query engine, reads Iceberg tables through Polaris |

## Streaming medallion

Bronze, silver, and gold are all continuously maintained -- there is no
batch step anywhere. Each layer is a set of Fluss primary-key tables (hot,
sub-second), tiered by the same tiering service into an Iceberg namespace of
the same name (cold, queried by Trino).

| Layer | Tables | Semantics | Built by |
|---|---|---|---|
| `bronze` | the 6 source tables | **Current state** of each Postgres table (UPDATE/DELETE applied), 1:1 with the source | The CDC job; a `route:` block in `flink/postgres-to-fluss.yaml` lands `public.*` in `bronze.*` |
| `silver` | `customers`, `products`, `orders`, `order_lines`, `payments` | Trimmed / case-normalised text, `DECIMAL` money alongside integer cents, `order_date`, and `order_lines` = items joined with orders, products, categories | `flink/medallion/silver_job.sql` -- one Flink SQL job reading the bronze changelog |
| `gold` | `daily_revenue_by_category`, `customer_lifetime_value`, `order_status_summary` | Business aggregates, excluding cancelled orders where relevant | `flink/medallion/gold_job.sql` -- one Flink SQL job reading the silver changelog |

Design choices worth knowing:

- **Silver uses regular streaming joins, not Fluss lookup joins.** A lookup
  join is cheaper (no Flink state) but not retroactive: renaming a category
  wouldn't update order lines already emitted, and during the initial
  snapshot an order item that arrives before its product would be dropped
  for good. A regular join re-emits whenever either side changes, so silver
  stays an exact function of bronze.
- **Gold is retraction-safe.** Fluss PK tables emit a full changelog
  (`-U/+U/-D`), and `COUNT`, `SUM`, `COUNT(DISTINCT)`, `MIN`/`MAX` all
  retract. Cancelling an order moves its revenue out of gold in place; a
  group whose last row is retracted is deleted, matching Postgres's
  `GROUP BY`.
- **Money is kept in integer cents** next to the `DECIMAL` columns, so gold
  can be reconciled exactly against Postgres (`make verify-medallion`).

## Quickstart

```bash
make up                # starts storage/catalog/Fluss/Flink/Trino
# wait ~30s for polaris-bootstrap and fluss-coordinator to settle
make submit-pipeline   # submits the Postgres -> Fluss CDC job
make submit-tiering    # submits 2 Fluss -> Iceberg tiering jobs (TIERING_JOBS=2)
make submit-medallion  # waits for bronze, then submits bronze->silver and silver->gold
make verify            # bronze: row-count check + live insert/update/delete test
make verify-medallion  # gold reconciled against Postgres + live retraction test
```

Or `make smoke` to do all of the above with built-in waits.

**Upgrading an existing stack**: run `make reset` and
`docker compose build flink-jobmanager flink-taskmanager` first. `make up`
reuses a cached Flink image, and the old `public.*` tables and CDC slot
offsets don't carry over to `bronze.*`.

UIs: Flink `:8082`, Trino `:8080`, Polaris `:8181`, SeaweedFS S3 `:8333`
(Admin UI `:23646`).

## What this actually proves

Everything below was reproduced against this repo's own running stack via
`scripts/verify_pipeline.sh`:

- **Initial snapshot**: all 6 tables, exact row-count match between Postgres
  and Iceberg (queried through Trino) after the CDC pipeline's snapshot phase.
- **Live CDC + tiering**: an INSERT, UPDATE, and DELETE against
  `source-postgres` each land in Iceberg (visible via Trino) within about
  30–150 seconds — Postgres → Fluss is near-instant; Fluss → Iceberg is
  bounded by the tiering service's per-table freshness target (30s) times up
  to 6 tables, since one tiering job round-robins across all tables rather
  than tiering them in parallel.
- **Fluss's Iceberg tiering produces standard Iceberg tables**: verified by
  reading the raw S3 objects directly (`mc ls` against SeaweedFS shows
  Parquet data files, Avro manifests, and JSON metadata files — the same
  layout `lakehouse-iceberg-batch` produces via dlt) and by querying them
  from Trino, an engine with zero Fluss-specific code.
- **Two genuine upstream bugs (in Flink CDC's transform module and its
  schema-type-inference code) found and worked around**, not glossed over —
  see [Findings](#findings--what-broke-and-why).
- **Streaming medallion, reconciled exactly against the source**
  (`make verify-medallion`): all 14 tables (6 bronze, 5 silver, 3 gold)
  land in their Iceberg namespaces; every gold table equals the same
  aggregation computed directly on Postgres; and after a live order
  cancel/un-cancel, a category rename, an INSERT, and a DELETE, gold
  converges back to Postgres through bronze -> silver -> gold -> Iceberg in
  well under the 480s timeout (whole script: 44-100s with 2 tiering jobs).

## Findings — what broke and why

Findings 1-5 had to be fixed before the CDC pipeline worked, and 6-9
before the [medallion](#streaming-medallion) did, in the order they
were hit. Each is a real, reproducible finding against the specific version
combination used here (Fluss 0.9.1-incubating, Flink CDC 3.6.0, Flink 1.20,
Iceberg 1.10.1), not a design preference.

### 1. Fluss doesn't support `TIMESTAMP WITH TIME ZONE`, and the documented workaround doesn't work either

Postgres's `TIMESTAMPTZ` maps to Flink CDC's `ZonedTimestampType`. The Fluss
sink connector rejects it outright:
`Unsupported data type in fluss TIMESTAMP(6) WITH TIME ZONE`.

The obvious fix — `CAST(col AS TIMESTAMP)` in a `transform` rule — doesn't
work either. It hits a **separate, confirmed upstream bug** in Flink CDC
3.6.0's transform module: a `NumberFormatException` in
`BinaryRecordData.getZonedTimestamp`, thrown while `PreTransformOperator` is
filling in the row's *other* fields, before the cast expression itself ever
runs. (Same underlying issue independently reported against a different sink
in [flink-cdc#4163](https://github.com/apache/flink-cdc/discussions/4163).)

**Fix applied**: `postgres/init/01_schema.sql` uses plain `TIMESTAMP` columns,
not `TIMESTAMPTZ`, avoiding the type entirely rather than working around a
broken code path. Worth re-testing on a newer Flink CDC release.

### 2. Neither Fluss's CoordinatorServer nor the tiering service ship the jars they need for S3-backed Iceberg

`apache/fluss:0.9.1-incubating`'s image bundles `fluss-lake-iceberg` (the
connector glue) but nothing else. Two separate missing-jar failures, in order:

- `ClassNotFoundException: org.apache.hadoop.conf.Configurable` — Iceberg's
  `CatalogUtil.loadFileIO` classloading path touches Hadoop's `Configurable`
  class even when the actual FileIO implementation used is S3-based, not
  Hadoop-based. Needs a Hadoop jar on the classpath regardless.
- `Cannot find constructor for interface org.apache.iceberg.io.FileIO ...
  Missing org.apache.iceberg.aws.s3.S3FileIO` — **`iceberg-aws-bundle`,
  despite the name, does not contain `S3FileIO`.** It's just the shaded AWS
  SDK dependencies `S3FileIO` needs at runtime. The class itself lives in
  the separate `iceberg-aws` jar. Confirmed by inspecting the bundle jar:
  zero `S3FileIO` class entries anywhere in it.

**Fix applied**: `fluss/Dockerfile` adds `iceberg-aws`, `iceberg-aws-bundle`,
`hadoop-apache`, and `failsafe` to `${FLUSS_HOME}/plugins/iceberg/` — needed
by the CoordinatorServer/TabletServer themselves (they open an Iceberg
catalog client directly, not just the separate Flink tiering job). The same
four jars are duplicated into `${FLINK_HOME}/lib` in `flink/Dockerfile` for
the tiering service, per Fluss's own docs.

Related: even with the right jars, Iceberg's `ResolvingFileIO` falls back to
`HadoopFileIO` for `s3://` paths unless told otherwise, which throws a
misleading `No FileSystem for scheme s3` (Hadoop's `S3AFileSystem` isn't on
the classpath by design here). Fixed by setting
`datalake.iceberg.io-impl: org.apache.iceberg.aws.s3.S3FileIO` explicitly —
undocumented in Fluss's own Iceberg integration page, found by reading the
actual exception's `DynConstructors` call chain.

### 3. Flink CDC needs an explicit checkpoint interval, or streaming silently never starts

Flink CDC's incremental snapshot framework
(`SnapshotSplitAssigner`) only transitions the Postgres source from
snapshot phase to streaming phase **after a checkpoint completes** following
the last snapshot chunk. Flink CDC 3.5+ removed the checkpointing defaults
that older docs assumed — Flink's own `flink-conf.yaml` in this image has no
checkpoint interval configured by default.

**Symptom**: the job logs
`Snapshot split assigner received all splits finished, waiting for a
complete checkpoint to mark the assigner finished` and then just... sits
there. Snapshot data flows fine. Live inserts after that point are silently
never picked up — no error, no warning, job stays `RUNNING`. Confirmed by
testing: an INSERT made after the pipeline started never reached Fluss until
this was fixed.

**Fix applied**: `flink/run-cdc-pipeline.sh` passes
`-D execution.checkpointing.interval=10s` to `flink-cdc.sh`.

### 4. Postgres's default `REPLICA IDENTITY` crash-loops the whole pipeline on the first UPDATE or DELETE

With Postgres's default `REPLICA IDENTITY DEFAULT`, UPDATE/DELETE WAL records
only carry the **primary key** in the "before" image — every other column
comes through as `NULL`. Flink CDC 3.6.0's schema-type-inference code
(`DebeziumSchemaDataTypeInference.inferStruct`) throws a bare
`NullPointerException` when it tries to infer a type from those null,
non-PK fields. This isn't a graceful degradation — it **crash-loops the
entire Flink job** (all 4 pipeline tasks restart, repeatedly, on every
checkpoint-restore attempt) the instant any UPDATE or DELETE hits a table
with the default replica identity.

**Fix applied**: `postgres/init/01_schema.sql` sets
`ALTER TABLE ... REPLICA IDENTITY FULL` on every captured table. This is
also good practice for the sibling Debezium-based repos, which need it for
the same reason (full old-row detail on UPDATE/DELETE), even though Debezium
itself doesn't crash without it.

### 5. Compose creates containers (baking in `env_file`) before their dependency's output actually exists

`fluss-coordinator`/`fluss-tablet` need `FLUSS_CLIENT_ID`/`FLUSS_CLIENT_SECRET`,
generated by `polaris-bootstrap` at runtime into `polaris/creds.env`. The
natural approach — `env_file: ./polaris/creds.env` plus
`depends_on: polaris-bootstrap: condition: service_completed_successfully`
— looks correct but isn't reliable: Compose **creates** a container (which
snapshots `env_file`'s contents at that moment) as soon as dependency
conditions are satisfied, but on a cold `docker compose up`, was observed
creating `fluss-coordinator` a full 60 seconds *before* `polaris-bootstrap`
actually finished writing real values into `creds.env`. The coordinator then
started with empty credentials baked in — and because `restart:
unless-stopped` restarts the *same* container rather than recreating it, it
kept retrying with permanently empty values (`NotAuthorizedException:
invalid_client`) until manually recreated.

**Fix applied**: `fluss/docker-entrypoint-wrapper.sh` sources
`/polaris/creds.env` itself, every container start (initial start *and*
every subsequent restart), before handing off to the base image's own
`/docker-entrypoint.sh`. `docker-compose.yml` mounts `creds.env` as a
read-only volume instead of `env_file`, so the wrapper always reads current
contents at the moment it actually runs, not whatever existed at container
creation.

### 6. Fluss's Iceberg tiering can't write `DATE` columns, and one bad table stops tiering for all of them

A silver table with a `DATE` column (`order_date`) was created fine and the
silver job wrote to it fine, but both tiering jobs then failed with
`Failed to write Fluss record to Iceberg` /
`IllegalStateException: Not an instance of java.lang.Integer: 2026-09-07`.
Fluss 0.9.1's Iceberg writer hands Iceberg a date object, while Iceberg's
`DATE` internally expects an int day count. The tiering jobs run with
`NoRestartBackoffTimeStrategy`, so the job goes to `FAILED` and **every**
table stops tiering, bronze included. That surfaced first as a bronze
UPDATE that never reached Trino, not as anything date-related.

**Fix applied**: `order_date` is `STRING` (`DATE_FORMAT(created_at,
'yyyy-MM-dd')`) in `silver_tables.sql` and `gold_tables.sql`. ISO date
strings still sort and compare correctly. `DECIMAL` columns tier fine.

### 7. Composite primary keys need an explicit single `bucket.key` for Iceberg

`gold.daily_revenue_by_category` has `PRIMARY KEY (order_date, category_id)`.
A Fluss PK table's bucket key defaults to the whole primary key, and the
Iceberg lake integration rejects that at `CREATE TABLE`:
`UnsupportedOperationException: Only one bucket key is supported for Iceberg
at the moment`.

**Fix applied**: `'bucket.key' = 'category_id'` in the table's `WITH`
clause.

### 8. `CREATE TABLE IF NOT EXISTS` isn't re-runnable for datalake-enabled tables

Once a `table.datalake.enabled` table's Iceberg counterpart exists,
`CREATE TABLE IF NOT EXISTS` fails with `CatalogException: Table
silver.customers already exists`, even when the Fluss table also exists
(the same statement without `table.datalake.enabled` succeeds as a no-op).
Separately, `DROP DATABASE ... CASCADE` in the Fluss catalog drops the
Fluss tables but **leaves their Iceberg tables in Polaris**, so a
drop-and-recreate fails the same way. The orphans have to be deleted
through Polaris's REST API with the writer (`fluss_tiering`) principal,
because Trino's `analyst` principal is read-only.

**Fix applied**: DDL is split from the jobs (`*_tables.sql` vs
`*_job.sql`). `run-medallion.sh` creates a layer's tables only when none of
them exist, skips creation when all exist, and refuses to guess on a
partial layer.

### 9. `sql-client.sh -f` exits 0 when a statement fails

A failed statement prints `[ERROR]` and stops the file, but the process
exits 0, so a naive submit script reports success with no job running.
This hid the first silver failure, which was itself caused by `method` (a
`payments` column) being a reserved word in Flink SQL. It needs backticks.

**Fix applied**: `run-medallion.sh` greps the client's output for `[ERROR]`
and fails loudly. `method` is quoted in `silver_*.sql`.

### Also worth knowing (not bugs, just friction)

- **Docker named volumes are root-owned by default.** `apache/fluss`'s image
  runs as uid 9999; a fresh named volume mounted into it is owned by root,
  so the tablet server crash-loops on `Permission denied` writing its
  recovery checkpoint. Fixed with a `fluss-tablet-data-init` one-shot
  `chown` container ahead of the tablet server, same pattern
  `lakehouse-iceberg-batch` uses for `polaris-creds-init`.
- **`docker-compose.yml`'s `$$` escaping for Fluss's own `envsubst`.**
  Separately from finding 5 above, the `FLUSS_PROPERTIES` block itself
  references `$FLUSS_CLIENT_ID`/`$FLUSS_CLIENT_SECRET` (for
  `datalake.iceberg.credential`), which the Fluss image's own
  `docker-entrypoint.sh` resolves via `envsubst` over `server.yaml` at
  container start. Those had to be escaped as `$$FLUSS_CLIENT_ID` in
  `docker-compose.yml` so Compose leaves the literal `$FLUSS_CLIENT_ID`
  string alone at parse time instead of substituting an empty value itself.
- **SeaweedFS's STS/AssumeRole was never actually exercised.** Going in, the
  concern was that Polaris's credential-vending (STS AssumeRole against
  SeaweedFS) might hit the same instability flagged in
  [seaweedfs#8312](https://github.com/seaweedfs/seaweedfs/discussions/8312).
  In practice, this pipeline authenticates to SeaweedFS with static
  credentials throughout (`s3.access-key`/`s3.secret-key` for Fluss and
  Polaris; Trino's `iceberg.rest-catalog.vended-credentials-enabled=true`
  does request vended credentials from *Polaris*, but Polaris's own S3 calls
  to SeaweedFS use static keys, not STS). The STS risk never actually came
  up — worth flagging as an open question for anyone who *does* need
  SeaweedFS's STS path (e.g. scoped, short-lived credentials per query
  engine), since that code path is genuinely less mature.

## Comparison to `stream-debezium-kafka`

| | `stream-debezium-kafka` | This repo |
|---|---|---|
| CDC capture | Debezium (Kafka Connect source connector) | Flink CDC's built-in `postgres-cdc` pipeline connector |
| Transport/storage | Kafka topics (JSON, schema in payload) | Fluss log + PK tables (columnar, Arrow-based) |
| Sink wiring | Separate Kafka Connect sink connector (ClickHouse) | Fluss's datalake tiering is a first-class Fluss feature, not a bolt-on connector |
| Containers running | 5 (Kafka, Kafka Connect, kafka-ui, source, destination) | 10 (source, ZK, 2x Fluss, 2x Flink, SeaweedFS, Polaris + its Postgres, Trino) — more, because this repo also stands up the full lakehouse (Iceberg/Polaris/Trino/SeaweedFS) that Kafka would otherwise hand off to a separate downstream system |
| Point lookups on captured data | None (would need a separate KV store, e.g. Redis) | Native: `SELECT * FROM orders WHERE order_id = 1` against Fluss directly, sub-millisecond |
| Historical/cold data | External (ClickHouse, unbounded retention by default) | Native tiering to Iceberg, standard Parquet/Avro, queryable by any Iceberg-compatible engine |
| Schema evolution | Debezium's `auto.evolve` (opt-in, needs a ClickHouse grant, tested both ways — see that repo's `docs/architecture.md`) | Flink CDC's `schema-change.enabled` + Fluss's lenient schema evolution (add/drop/rename column) |
| Maturity | Debezium: 8+ years, huge community | Fluss: graduated to ASF Top-Level Project Aug 2026 — 4 real upstream bugs found in one afternoon of use, plus 3 more Fluss/Iceberg integration gaps (findings 6-8) once a medallion was built on it |

**The honest summary**: Fluss's core promise — collapsing Kafka + Debezium +
Kafka Connect + a lakehouse-sink connector into one system with native PK
lookups — genuinely holds up once configured correctly. But "configured
correctly" required finding and working around two real Flink CDC 3.6.0 bugs
and two missing/misdocumented dependency issues that aren't in Fluss's own
docs. Debezium's ecosystem has had a decade to sand those edges off; Fluss's
integration with Flink CDC's newer pipeline-connector framework clearly
hasn't yet, at least not for Postgres.

## Repo layout

```
docker-compose.yml                  All services
fluss/Dockerfile                    apache/fluss + the missing Iceberg/S3/Hadoop jars
fluss/docker-entrypoint-wrapper.sh  Sources polaris/creds.env at container start (finding 5)
flink/Dockerfile                    Flink 1.20 + Flink CDC + Fluss connectors + tiering jars
flink/postgres-to-fluss.yaml        The CDC pipeline definition
flink/run-cdc-pipeline.sh           Renders env vars into the pipeline YAML and submits it
flink/run-tiering-service.sh        Submits one Fluss -> Iceberg tiering service job (instance # arg)
flink/medallion/init.sql            Fluss catalog + streaming settings shared by the medallion jobs
flink/medallion/*_tables.sql        Silver / gold Fluss table DDL (datalake-enabled)
flink/medallion/*_job.sql           bronze -> silver and silver -> gold streaming jobs
flink/run-medallion.sh              Waits for bronze, creates missing layers, submits both jobs
polaris/bootstrap.sh                Creates the Polaris catalog, namespace, principals, grants
postgres/init/                      Source schema (REPLICA IDENTITY FULL, plain TIMESTAMP) + seed data
trino/etc/                          Trino config, including the Iceberg/Polaris catalog
scripts/verify_pipeline.sh          Bronze: row-count + live CDC/tiering verification
scripts/verify_medallion.sh         Gold reconciled against Postgres + live retraction test
```

## Production considerations

Named honestly, not glossed over:

- **`weed mini` is a single-node, all-in-one SeaweedFS instance.** Fine for
  this repo's purpose; production SeaweedFS wants separate master/volume/
  filer processes with replication, per SeaweedFS's own docs.
- **No HA for Flink or Fluss.** Single JobManager, single Fluss
  CoordinatorServer, one TabletServer, one ZooKeeper node — all single
  points of failure by design here, same "POC not production" framing as
  `lakehouse-iceberg-batch`.
- **Static S3 credentials throughout**, not the scoped STS credential-vending
  pattern `lakehouse-iceberg-batch` uses for Trino/Polaris/MinIO. Revisit if
  SeaweedFS's STS implementation matures (see the finding above).
- **Tiering freshness (30s per table, queued) won't scale to many
  tables or high-throughput tables without tuning** — `table.datalake.freshness`
  and the number of tiering service instances (`TIERING_JOBS`, 2 here for the
  medallion's 14 tables) are the levers to pull.
- **One bad table stops tiering for every table.** The tiering jobs run with
  no restart strategy, and a single table that fails to write (see the
  `DATE` finding) fails the whole job, so bronze stops reaching Iceberg too.
  Worth alerting on tiering job state.
- **Medallion join and aggregate state is unbounded** (no
  `table.exec.state.ttl`, heap state backend). Fine for this dataset; real
  volumes want RocksDB plus a TTL, or lookup joins where staleness is
  acceptable.
- **Postgres schema changes stop at bronze.** `schema-change.enabled` keeps
  bronze in sync, but silver and gold have fixed DDL, and dropping a column
  `silver_job.sql` reads will fail the silver job.
- **Resource footprint grew**: the TaskManager is now 4 GB with 8 slots.
- **No monitoring stack.** Fluss ships an observability quickstart
  (Prometheus/Grafana); not wired up here.
