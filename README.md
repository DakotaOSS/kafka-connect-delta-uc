# kafka-connect-delta

Kafka Connect sink that writes to Unity Catalog managed Delta tables via the Delta Kernel Java API, no Spark.

A `SinkTask` buffers Kafka records per topic-partition and commits each buffer as one Delta transaction against a Databricks **managed, catalog-managed** Delta table. Commits are coordinated through Unity Catalog (catalog commits + credential vending), so an external engine can append to a managed table without going through Databricks compute. The connector runs in the same Kafka Connect plane as your source connectors (Debezium, JDBC, HTTP) — there is no second Spark/Databricks runtime to operate.

## Status

v0.1, append-only. The managed-UC write path depends on two preview/evolving pieces:

- **Databricks Beta** — external writes to managed Delta tables are gated behind a workspace Preview toggle ("External Access to UC Managed Delta Table"). Requires DBR 16.4+.
- **Delta Kernel `@Evolving` write API** — write support shipped in Kernel 3.2 and is still marked `@Evolving`. Catalog-managed snapshot reads need Kernel 4.1+; this connector pins 4.2.0.

Pin versions and gate upgrades behind the test suite. UPDATE/DELETE/MERGE are not supported by the Kernel write API — see [CDC with Debezium](#cdc-with-debezium).

## Features

- **Managed UC writes via Delta Kernel** — appends to catalog-managed Delta tables, commit coordination through Unity Catalog, storage credentials via UC credential vending. No Spark.
- **Debezium flat + full nested envelope** — maps flat primitive/logical-type records, and full nested `before`/`after`/`source` structs to nested Delta struct columns.
- **Three flush dials** — `flush.size` (rows), `flush.bytes` (target file size), `flush.interval.ms` (max latency); whichever trips first commits.
- **Effectively-once** — each per-partition commit is stamped `SetTransaction(appId, kafkaOffset)`; replays after a crash are no-ops. Offsets are returned from `preCommit` only after the Delta commit succeeds.
- **Streaming low-latency commits** — the snapshot is loaded once per table and the post-commit snapshot is reused (no per-commit log re-read); backfill and checkpoint run off the commit path.

## How it works

`put()` buffers records per `(topic, partition)`. A flush converts the batch to a Kernel columnar batch, resolves the target UC table (vending READ_WRITE credentials and an `abfss://` path), writes Parquet through a Hadoop/ABFS engine, and commits one Delta version via `UnityCatalogCommitter` (stage under `_delta_log/_staged_commits/`, ratify through UC). Publish + checkpoint run async so per-commit latency stays flat as the table grows. `preCommit()` returns the durable offsets.

Full design and the commit protocol: [docs/SPEC.md](docs/SPEC.md).

## Quickstart

Build the connector jar. The module is in `delta-sink-connector/`.

Docker (no local JDK needed):

```bash
docker run --rm -v "$PWD/delta-sink-connector:/work" -w /work maven:3.9-eclipse-temurin-17 mvn -B package
```

Or with Maven on JDK 17:

```bash
cd delta-sink-connector && mvn -B package
```

Drop the jar on the Connect worker's plugin path, then POST a connector config:

```json
{
  "name": "delta-sink-orders",
  "config": {
    "connector.class": "io.dakotaoss.delta.DeltaSinkConnector",
    "tasks.max": "1",
    "topics": "orders",
    "databricks.workspace.url": "https://adb-1234567890.1.azuredatabricks.net",
    "databricks.token": "${file:/opt/secrets/databricks.properties:token}",
    "table.name.format": "main.ingestion.${topic}",
    "flush.size": "5000",
    "flush.bytes": "134217728",
    "flush.interval.ms": "5000"
  }
}
```

The token principal needs `EXTERNAL USE SCHEMA` on the target schema(s) — and it is the connector's only authorization boundary, so use a dedicated service principal scoped to just those schemas, with short-lived OAuth over a long-lived PAT (see [Least-privilege principal](docs/USAGE.md#least-privilege-principal)). One connector routes many topics to many tables — see [docs/USAGE.md](docs/USAGE.md) for `${topic[N]}` segment tokens and `topic.to.table` overrides. Per-partition `SetTransaction` keeps delivery effectively-once; UC conflict arbitration is the safety net for same-table concurrency.

## Configuration

| Key | Default | Description |
|---|---|---|
| `databricks.workspace.url` | — | Workspace base URL for the UC REST API. |
| `databricks.token` | — | Bearer token (PAT/OAuth/AAD). Principal needs `EXTERNAL USE SCHEMA`. |
| `table.name.format` | `main.ingestion.${topic}` | Default template. `${topic}` = whole topic; `${topic[N]}` = its Nth dot-segment (0-indexed), so a structured topic maps to any `catalog.schema.table` (e.g. `bronze.${topic[1]}.${topic[3]}`). Each substituted value must be a valid identifier (`[A-Za-z0-9_]`, ≤255) or routing is rejected. |
| `topic.to.table` | (none) | Per-topic overrides that win over the template: `<topic>:<catalog>.<schema>.<table>,...` |
| `flush.size` | `500` | Rows buffered per partition before commit. `0` disables the row dial. |
| `flush.bytes` | `0` | Flush at approx buffered bytes, for target file size (e.g. `134217728` = 128 MiB). `0` disables. |
| `flush.interval.ms` | `5000` | Max ms to buffer a partition before commit. Drives micro-batch latency. |
| `partition.columns` | (none) | Partition columns, used only when this connector creates a new table. |

## CDC with Debezium

The connector is an append-only bronze writer. The Kernel write API has no DML, so deletes and updates are not applied in the connector — they are carried as change events and merged downstream in Databricks (AUTO CDC or `MERGE` on a SQL warehouse). Bronze stays append-only.

Flatten the Debezium envelope with the `ExtractNewRecordState` SMT, but keep the fields a downstream MERGE needs — the op and the source LSN/SCN:

```json
"transforms": "unwrap",
"transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
"transforms.unwrap.drop.tombstones": "false",
"transforms.unwrap.delete.handling.mode": "rewrite",
"transforms.unwrap.add.fields": "op,source.lsn,source.ts_ms"
```

Rule: keep `op` plus a **monotonic** source sequence (LSN for SQL Server/Postgres, SCN for Oracle) on every bronze row. The downstream MERGE dedupes and orders by that sequence (`ROW_NUMBER() OVER (PARTITION BY pk ORDER BY seq DESC)`); without it you cannot resolve out-of-order or duplicate events. Full nested-envelope ingestion (without the SMT) also works — `before`/`after`/`source` map to nested struct columns.

See [docs/SPEC.md](docs/SPEC.md) for the merge templates and the full CDC strategy.

## Benchmarks

Live runs against a catalog-managed UC table writing a Debezium envelope payload.

![throughput](docs/benchmarks/throughput-vs-volume.png)

![latency](docs/benchmarks/latency-timeline-100m.png)

- **235k rows/s** sustained writing 100M rows (424 s, 50 commits).
- **271k rows/s** peak at 2.5M-row batches.
- **~2 s p50** commit latency at small batches; latency stays flat across all 50 commits of the 100M run.

These are a floor — the harness runs in a single container cross-region to ADLS + UC, so every commit pays WAN latency that an in-region worker removes. Detail, charts, and raw CSVs: [docs/benchmarks/README.md](docs/benchmarks/README.md).

## Limitations

- Append-only. No UPDATE/DELETE/MERGE (Kernel write API limitation); do DML downstream.
- Unpartitioned writes. `partition.columns` applies only at table creation; partitioned append is an extension point.
- Nested STRUCT is supported; top-level ARRAY/MAP columns are rejected.
- Azure/ADLS Gen2 (`abfss://`) only for the live path.
- Depends on the Databricks Beta and the `@Evolving` Kernel write API (see [Status](#status)).
- Credential refresh for very long-running tasks is still being hardened.

## License

MIT — see [LICENSE](LICENSE).

---

[docs/SPEC.md](docs/SPEC.md) · [CONTRIBUTING.md](CONTRIBUTING.md)
