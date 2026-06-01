<#
  Durable, observable batch-size sweep - the harness behind batch-sweep.csv / volume-scaling.csv.
  Windows mirror of run-sweep.sh; see that file's header for the full rationale.

  run-benchmark.ps1 does ONE point against a pre-created table. A sweep is many points back-to-back,
  and the live failure modes are all silent: a token expiring mid-run becomes a retry storm that looks
  like a hang; reusing one table lets a partially-committed point desync the UC commit coordinator so
  the next point storms too; buffered output leaves you blind. This runner closes all three -
    durable    one FRESH uniquely-named table per point (dropped after), plus an optional token re-mint
               before every point so a long sweep can't outlive its token.
    observable per-commit [BENCH] lines stream live; one log file per point under sweep-logs/.
    fail-fast  each point runs in a detached container with a watchdog that docker-kills it past the
               timeout; a storming/hung point dies loudly and the sweep moves on. Any fail => exit 1.

  Required env:
    DATABRICKS_HOST      https://adb-xxxx.azuredatabricks.net
    BENCH_WAREHOUSE_ID   SQL warehouse id - used to CREATE/DROP the per-point tables
    DATABRICKS_TOKEN     PAT or Entra token (required unless BENCH_MINT_CMD is set)
  Optional:
    BENCH_MINT_CMD   command printing a fresh token to stdout; re-run before EVERY point. e.g.:
                       $env:BENCH_MINT_CMD='az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken -o tsv'
    BENCH_SCHEMA     catalog.schema for the fresh tables   (default main.default)
    BENCH_BATCHES    space-separated batch sizes            (default "100000 250000 500000 1000000 2500000")
    BENCH_SWEEP_ROWS rows per point                         (default 5000000)
    BENCH_TIMEOUT    per-point hard timeout, seconds        (default 1800)
    OUT_CSV          results file (appended)                (default batch-sweep.csv)
#>
$ErrorActionPreference = "Stop"
if (-not $env:DATABRICKS_HOST)    { throw "set DATABRICKS_HOST" }
if (-not $env:BENCH_WAREHOUSE_ID) { throw "set BENCH_WAREHOUSE_ID (SQL warehouse for CREATE/DROP of per-point tables)" }
$schema  = if ($env:BENCH_SCHEMA)     { $env:BENCH_SCHEMA }     else { "main.default" }
$batches = if ($env:BENCH_BATCHES)    { $env:BENCH_BATCHES -split '\s+' } else { @("100000","250000","500000","1000000","2500000") }
$rows    = if ($env:BENCH_SWEEP_ROWS) { $env:BENCH_SWEEP_ROWS } else { "5000000" }
$timeout = if ($env:BENCH_TIMEOUT)    { [int]$env:BENCH_TIMEOUT } else { 1800 }
$outCsv  = if ($env:OUT_CSV)          { $env:OUT_CSV }          else { "batch-sweep.csv" }

$root   = (Resolve-Path "$PSScriptRoot/../..").Path
$logDir = Join-Path $PSScriptRoot "sweep-logs"; New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$runTag = Get-Date -Format "yyyyMMddHHmmss"

function Mint {
  if ($env:BENCH_MINT_CMD) {
    $t = (Invoke-Expression $env:BENCH_MINT_CMD)
    if (-not $t) { throw "[SWEEP] mint produced an empty token" }
    $env:DATABRICKS_TOKEN = ($t | Select-Object -First 1).Trim()
  } elseif (-not $env:DATABRICKS_TOKEN) { throw "set DATABRICKS_TOKEN or BENCH_MINT_CMD" }
}

function Uc-Sql([string]$sql) { # run synchronously on the warehouse; throw if not SUCCEEDED
  $body = @{ statement = $sql; warehouse_id = $env:BENCH_WAREHOUSE_ID; wait_timeout = "30s" } | ConvertTo-Json
  $resp = Invoke-RestMethod -Method Post -Uri "$($env:DATABRICKS_HOST)/api/2.0/sql/statements" `
    -Headers @{ Authorization = "Bearer $($env:DATABRICKS_TOKEN)" } -ContentType "application/json" -Body $body
  if ($resp.status.state -ne "SUCCEEDED") { throw "[SWEEP] SQL state=$($resp.status.state)" }
}

function Ddl([string]$t) {
  "CREATE TABLE IF NOT EXISTS $t (before STRUCT<id:INT,name:STRING,email:STRING>, after STRUCT<id:INT,name:STRING,email:STRING>, op STRING, ts_ms BIGINT, source STRUCT<db:STRING,table_name:STRING,lsn:BIGINT>) TBLPROPERTIES ('delta.feature.catalogManaged'='supported')"
}

if (-not (Test-Path $outCsv) -or (Get-Item $outCsv).Length -eq 0) { "batch,rows_per_sec,commits,p50_s,p99_s" | Out-File -Encoding utf8 $outCsv }
$failed = 0
Write-Host "[SWEEP] $runTag | rows/point=$rows timeout=${timeout}s batches: $batches"

foreach ($b in $batches) {
  Mint
  $table = "$schema.bench_sweep_${b}_${runTag}"
  $cname = "bench_sweep_${b}_${runTag}"
  $log   = Join-Path $logDir "batch-$b-$runTag.log"
  Write-Host "[SWEEP] ===== batch=$b -> $table ====="
  try { Uc-Sql (Ddl $table) } catch { Write-Host "[SWEEP] FAIL batch=${b}: could not create table ($_)"; $failed = 1; continue }

  # detached container + foreground `logs -f` (live + tee), with a watchdog job that kills it on timeout.
  docker run -d --name $cname --init `
    -v "$root/delta-sink-connector:/work" -v "$env:USERPROFILE/.m2:/root/.m2" -w /work `
    -e DATABRICKS_HOST=$env:DATABRICKS_HOST -e DATABRICKS_TOKEN=$env:DATABRICKS_TOKEN `
    -e BENCH_TABLE=$table -e BENCH_ROWS=$rows -e BENCH_BATCH=$b `
    maven:3.9-eclipse-temurin-17 `
    mvn -B "-Dtest=BenchmarkTest" "-Djacoco.skip=true" "-DargLine=-Xmx3g" test | Out-Null
  $watch = Start-Job { param($n,$t) Start-Sleep -Seconds $t; docker kill $n 2>$null } -ArgumentList $cname,$timeout
  & docker logs -f $cname 2>&1 | Tee-Object -FilePath $log
  Stop-Job $watch -ErrorAction SilentlyContinue; Remove-Job $watch -Force -ErrorAction SilentlyContinue
  $rc = [int](docker inspect -f '{{.State.ExitCode}}' $cname)
  docker rm $cname | Out-Null

  if ($rc -eq 137 -or $rc -eq 143) { Write-Host "[SWEEP] FAIL batch=${b}: KILLED after ${timeout}s (likely a commit storm) - see $log"; $failed = 1 }
  elseif ($rc -ne 0) { Write-Host "[SWEEP] FAIL batch=${b}: exit $rc - see $log"; $failed = 1 }
  else {
    $line = Select-String -Path $log -Pattern '\[BENCH-RESULT\]' | Select-Object -First 1
    if (-not $line) { Write-Host "[SWEEP] FAIL batch=${b}: build passed but no [BENCH-RESULT] - see $log"; $failed = 1 }
    else {
      $m = [regex]::Match($line.Line, 'rows_per_sec=(\d+).*commits=(\d+).*p50=(\d+) p99=(\d+)')
      $rps=$m.Groups[1].Value; $com=$m.Groups[2].Value; $p50=[double]$m.Groups[3].Value; $p99=[double]$m.Groups[4].Value
      "{0},{1},{2},{3:N2},{4:N2}" -f $b,$rps,$com,($p50/1000),($p99/1000) | Add-Content $outCsv
      Write-Host "[SWEEP] ok batch=${b}: $rps rows/s, $com commits, p50=${p50}ms p99=${p99}ms"
    }
  }
  try { Uc-Sql "DROP TABLE IF EXISTS $table" } catch { Write-Host "[SWEEP] warn: drop of $table failed (clean up by hand)" }
}

Write-Host "[SWEEP] done -> $outCsv"; Get-Content $outCsv
exit $failed
