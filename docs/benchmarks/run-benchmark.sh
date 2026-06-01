#!/usr/bin/env bash
# Reproduce the connector benchmark with only Docker installed — no local Java/Maven.
#
# Required env:
#   DATABRICKS_HOST   https://adb-xxxx.azuredatabricks.net
#   DATABRICKS_TOKEN  a PAT, or an Entra token:
#                       az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken -o tsv
#   BENCH_TABLE       catalog.schema.table — a catalog-managed table with the envelope schema (see bench-table.sql).
#                     Use a FRESH table per full run: one left in a partially-committed state from an
#                     interrupted run can desync the UC commit coordinator and make commits retry.
# Optional:
#   BENCH_ROWS   total rows to write   (default 1000000)
#   BENCH_BATCH  rows per commit       (default 1000000)
#
# Example:
#   DATABRICKS_HOST=https://adb-123.4.azuredatabricks.net \
#   DATABRICKS_TOKEN=$(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken -o tsv) \
#   BENCH_TABLE=main.default.bench_cdc BENCH_ROWS=10000000 BENCH_BATCH=1000000 \
#   ./run-benchmark.sh
set -euo pipefail

: "${DATABRICKS_HOST:?set DATABRICKS_HOST}"
: "${DATABRICKS_TOKEN:?set DATABRICKS_TOKEN}"
: "${BENCH_TABLE:?set BENCH_TABLE (catalog.schema.table; catalog-managed, envelope schema — see bench-table.sql)}"
ROWS="${BENCH_ROWS:-1000000}"
BATCH="${BENCH_BATCH:-1000000}"

# repo root, two levels up from docs/benchmarks/
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

docker run --rm \
  -v "$ROOT/delta-sink-connector:/work" -v "${HOME}/.m2:/root/.m2" -w /work \
  -e DATABRICKS_HOST -e DATABRICKS_TOKEN \
  -e BENCH_TABLE -e BENCH_ROWS="$ROWS" -e BENCH_BATCH="$BATCH" \
  maven:3.9-eclipse-temurin-17 \
  mvn -B -Dtest=BenchmarkTest -Djacoco.skip=true -DargLine=-Xmx3g test
