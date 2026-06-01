# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

First cut, not yet tagged: the version is `0.1.0-SNAPSHOT` and no `v0.1.0` release
exists. Append-only bronze writer — a Kafka Connect sink that commits Debezium/Kafka
records into Databricks Unity Catalog **managed, catalog-managed** Delta tables through
the Delta Kernel Java API. No Spark, no second compute plane — the write runs in the
same Connect worker as the source connectors.

Depends on the Databricks **External Access to UC Managed Delta Table** Beta
(workspace Previews toggle), DBR 16.4+, and the `@Evolving` Kernel write API
(Kernel pinned to 4.2.0). See [docs/SPEC.md](docs/SPEC.md) for the design and risks.

### Added

- **Managed catalog-managed UC writes via Delta Kernel.** Each per-partition flush
  is one Delta transaction against a `delta.feature.catalogManaged=supported` table.
  Commit coordination moves off the filesystem to Unity Catalog: a
  `UnityCatalogCommitter` (Kernel `CatalogCommitter`) stages the commit file under
  `_delta_log/_staged_commits/`, ratifies it through UC (first-writer-wins), then
  backfills/publishes the ratified file into the numbered `_delta_log` and
  checkpoints to keep reads O(1) in table history. Storage credentials are
  short-lived, vended per table by UC (READ_WRITE), held as `char[]` in a process-wide
  `VendedSasStore` (never in the Hadoop `Configuration`) and handed to ABFS by a per-host
  `VendedSasTokenProvider`, so one cached FileSystem per host serves many tables. No
  long-lived storage secret, no stage bucket.
- **Debezium flat + full nested envelope.** Flat primitive/logical-type rows
  (post-`ExtractNewRecordState`) map to flat Delta columns; full nested
  `before`/`after`/`source` structs map to nested Delta struct columns. Schema
  mapping and record conversion recurse into STRUCTs.
- **Three flush dials, whichever trips first.** `flush.size` (rows), `flush.bytes`
  (approx buffered bytes, for target file size), `flush.interval.ms` (max latency).
  `flush.interval.ms` defaults to 5s — the max-latency SLA, enforced by a scheduler
  so queued rows commit within the interval under light traffic.
- **Effectively-once delivery.** Each per-partition commit is stamped
  `SetTransaction(appId, kafkaOffset)`; replays of an already-applied
  `(appId, version)` are rejected by Kernel and treated as no-ops. `preCommit`
  returns a partition's offset only after that partition's Delta commit succeeds.
- **Streaming low-latency write path.** The snapshot is loaded once per table and
  the post-commit snapshot is reused as the base for the next append (no per-commit
  log re-read). Maintenance (publish/backfill + checkpoint) runs on a single-thread
  executor off the commit path, so per-commit latency stays flat as the table grows.
- **Proactive credential refresh.** A cached catalog-managed table is re-resolved
  (re-vending UC credentials and rebuilding engine/committer/snapshot) once it is
  ~40 min old, before the ~1h vended-SAS TTL expires; a flush failure also re-vends
  reactively as a fallback. The bearer token is read from config per request via a
  `Supplier<String>`, so a re-minted PAT/AAD token is picked up without a restart.
- **Fail-closed poison-record / DLQ handling.** Records with a null/non-Struct value
  or a schema differing from the batch reference, and whole-batch write failures, are
  routed to Connect's errant-record reporter (DLQ) when one is configured; with no
  reporter the task fails rather than silently advancing the offset past unwritten
  bronze CDC data. Causes are redacted before they reach DLQ headers or task logs.
- **Backpressure ceiling.** A hard cap of 1,000,000 rows buffered across all
  partitions; once exceeded `put` throws a `RetriableException` so Connect pauses and
  re-delivers, bounding heap growth when a flush stalls (e.g. a UC outage).
- **commitInfo enrichment.** The committer populates the three controllable
  `commitInfo` fields so the Delta history reads like a Spark-written table:
  `operationMetrics` (`numOutputRows` / `numFiles` / `numOutputBytes`),
  `operationParameters` (`mode=Append`), and `isBlindAppend=true`.
- **Benchmarks.** Live runs against a managed catalog-managed UC table writing a
  Debezium envelope payload: 235k rows/s sustained over 100M rows (424 s, 50
  commits), 271k rows/s peak at 2.5M-row batches, ~2 s p50 commit at small batches,
  latency flat across the full run. Data, charts, and raw CSVs under
  [docs/benchmarks/](docs/benchmarks/README.md).

### Known limitations

- Append-only. No UPDATE/DELETE/MERGE (Kernel write API is blind-append); do DML
  downstream in Databricks (Lakeflow AUTO CDC or `MERGE` via the SQL warehouse).
- Unpartitioned writes. `partition.columns` applies only at table creation.
- Nested STRUCT supported; top-level ARRAY/MAP columns are rejected at schema-map time.
- Azure/ADLS Gen2 (`abfss://`) only for the live path.

[Unreleased]: https://github.com/DakotaOSS/kafka-connect-delta-uc/commits/main
