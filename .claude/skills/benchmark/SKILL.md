---
name: benchmark
description: Run the live throughput/latency benchmark against a catalog-managed Delta table (needs DATABRICKS_HOST/DATABRICKS_TOKEN/BENCH_TABLE). Use to reproduce or refresh the numbers in docs/benchmarks.
---

# benchmark

Wraps `docs/benchmarks/run-benchmark.{sh,ps1}`, which run `BenchmarkTest` in Docker (no local Java).
The target must be a catalog-managed table with the envelope schema — see `docs/benchmarks/bench-table.sql`.

PowerShell:

    $env:DATABRICKS_HOST="https://adb-xxxx.azuredatabricks.net"
    $env:DATABRICKS_TOKEN=(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken -o tsv)
    $env:BENCH_TABLE="catalog.schema.table"; $env:BENCH_ROWS="10000000"
    ./docs/benchmarks/run-benchmark.ps1

Optional env: `BENCH_ROWS` (total rows, default 1e6), `BENCH_BATCH` (rows per commit, default 1e6).
Results append to `docs/benchmarks/*.csv`; regenerate the charts with `docs/benchmarks/make_charts.py`.
