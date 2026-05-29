<#
  Reproduce the connector benchmark with only Docker installed — no local Java/Maven.

  Required env:
    DATABRICKS_HOST   https://adb-xxxx.azuredatabricks.net
    DATABRICKS_TOKEN  a PAT, or an Entra token:
                        az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken -o tsv
    BENCH_TABLE       catalog.schema.table — a catalog-managed table with the envelope schema (see bench-table.sql)
  Optional:
    BENCH_ROWS   total rows to write   (default 1000000)
    BENCH_BATCH  rows per commit       (default 1000000)

  Example:
    $env:DATABRICKS_HOST="https://adb-123.4.azuredatabricks.net"
    $env:DATABRICKS_TOKEN=(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken -o tsv)
    $env:BENCH_TABLE="main.default.bench_cdc"; $env:BENCH_ROWS="10000000"
    ./run-benchmark.ps1
#>
$ErrorActionPreference = "Stop"
if (-not $env:DATABRICKS_HOST)  { throw "set DATABRICKS_HOST" }
if (-not $env:DATABRICKS_TOKEN) { throw "set DATABRICKS_TOKEN" }
if (-not $env:BENCH_TABLE)      { throw "set BENCH_TABLE (catalog.schema.table; catalog-managed, envelope schema)" }
$rows  = if ($env:BENCH_ROWS)  { $env:BENCH_ROWS }  else { "1000000" }
$batch = if ($env:BENCH_BATCH) { $env:BENCH_BATCH } else { "1000000" }

$root = (Resolve-Path "$PSScriptRoot/../..").Path

docker run --rm `
  -v "$root/delta-sink-connector:/work" -v "$env:USERPROFILE/.m2:/root/.m2" -w /work `
  -e DATABRICKS_HOST=$env:DATABRICKS_HOST -e DATABRICKS_TOKEN=$env:DATABRICKS_TOKEN `
  -e BENCH_TABLE=$env:BENCH_TABLE -e BENCH_ROWS=$rows -e BENCH_BATCH=$batch `
  maven:3.9-eclipse-temurin-17 `
  mvn -B "-Dtest=BenchmarkTest" "-Djacoco.skip=true" "-DargLine=-Xmx3g" test
