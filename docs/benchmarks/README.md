# Benchmark results

Live runs against a real Databricks **managed, catalog-managed** Delta table (Unity Catalog,
`canadacentral`), writing a realistic **Debezium envelope** payload — nested `before`/`after`/`source`
structs — through the connector's streaming write path (snapshot reuse + asynchronous
backfill/checkpoint).

> **Read these as a floor, not a ceiling.** The harness runs in a single local Docker container
> talking cross-region to ADLS + Unity Catalog, so every commit pays WAN round-trip latency; a Kafka
> Connect worker co-located with the workspace region removes most of it. Synthetic rows draw from
> ~10k distinct entities, so on-disk files compress better than production-cardinality data would.

## Highlights

- **100M rows in ~7 min** (424 s, 50 commits) — **235k rows/s sustained** to a managed UC table.
- **271k rows/s peak** at large (2.5M-row) batches.
- **~2 s p50 commit latency** at small batches — a 5 s max-latency flush cadence is comfortable even
  from this out-of-region harness, and tighter in-region.
- **Latency stays flat at scale** — across all 50 commits of the 100M run there is no upward trend;
  appends are O(1) in table history thanks to backfill + periodic checkpoints.

## Scaling

Throughput rises into steady state and then holds, all the way to 100M rows.

![Throughput vs volume](throughput-vs-volume.png)

## Batching: throughput vs. latency vs. file size

Bigger batches amortize the fixed per-commit cost (snapshot load + Parquet + UC-coordinated commit),
trading commit latency and file count for throughput. The connector exposes this directly via
`flush.size` (rows), `flush.bytes` (target file size), and `flush.interval.ms` (max latency) —
whichever trips first.

![Throughput vs batch size](throughput-vs-batch.png)

![Commit latency vs batch size](latency-vs-batch.png)

## Latency holds flat at scale

Per-commit synchronous latency over the full 100M-row run — no growth as the table accumulates 50
versions (publish + checkpoint run off the commit path).

![Per-commit latency across 100M rows](latency-timeline-100m.png)

<details>
<summary><b>Full data</b></summary>

**Batch-size sweep (5M rows, 1M-row reference excluded):**

| batch (rows) | rows/sec | commits | p50 commit | p99 commit | files | total size |
|---:|---:|---:|---:|---:|---:|---:|
| 100,000 | 29,641 | 50 | 1.9 s | 5.3 s | 50 | 88.7 MB |
| 250,000 | 47,974 | 20 | 5.0 s | 10.4 s | 20 | 69.0 MB |
| 500,000 | 85,591 | 10 | 5.5 s | 10.8 s | 10 | 62.4 MB |
| 1,000,000 | 200,904 | 5 | 3.9 s | 6.9 s | 5 | 59.1 MB |
| 2,500,000 | 270,704 | 2 | 8.4 s | 8.4 s | 2 | 57.1 MB |

**Volume scaling (1M-row batches; 100M uses 2M-row batches):**

| rows | rows/sec | commits | p50 commit | files | total size |
|---:|---:|---:|---:|---:|---:|
| 100,000 | 16,020 | 1 | 5.0 s | 1 | 1.8 MB |
| 1,000,000 | 107,064 | 1 | 7.9 s | 1 | 11.8 MB |
| 10,000,000 | 203,373 | 10 | 4.1 s | 10 | 118 MB |
| 100,000,000 | 235,853 | 50 | 7.5 s | 50 | 1.15 GB |

</details>

## Reproduce

Only **Docker** is required — no local Java or Maven. Create the bench table
([`bench-table.sql`](bench-table.sql)) in Databricks, set credentials, and run:

```bash
export DATABRICKS_HOST="https://adb-xxxx.azuredatabricks.net"
export DATABRICKS_TOKEN=$(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken -o tsv)
export BENCH_TABLE="main.default.bench_cdc"
export BENCH_ROWS=10000000 BENCH_BATCH=1000000
./run-benchmark.sh          # Windows: ./run-benchmark.ps1
```

It runs `BenchmarkTest` in a `maven:3.9-eclipse-temurin-17` container against the connector module and
prints per-commit + summary `[BENCH-RESULT]` lines. Regenerate the charts from the CSVs with
`uv run --with matplotlib --with numpy python make_charts.py`.

_Raw data: [`batch-sweep.csv`](batch-sweep.csv), [`volume-scaling.csv`](volume-scaling.csv),
[`latency-timeline-100m.csv`](latency-timeline-100m.csv)._
