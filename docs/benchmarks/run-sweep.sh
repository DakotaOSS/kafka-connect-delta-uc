#!/usr/bin/env bash
# Durable, observable batch-size sweep — the harness behind batch-sweep.csv / volume-scaling.csv.
#
# run-benchmark.sh does ONE point against a pre-created table. A sweep is many points back-to-back,
# and the failure modes that bit us live were all silent: a token expiring mid-run turns into a retry
# storm that looks like a hang; reusing one table across points lets a partially-committed run desync
# the UC commit coordinator so the next point storms too; and buffered docker output leaves you blind.
# This runner closes all three:
#   durable   — one FRESH uniquely-named table per point (no cross-point desync), dropped after; and
#               an optional re-mint of the token before each point so a long sweep can't outlive it.
#   observable— per-commit [BENCH] lines stream live (tee, unbuffered), one log file per point.
#   fail-fast — each point is wrapped in a hard timeout; a storming/hung point is killed loudly and
#               the sweep moves on, instead of wedging forever. Any failed point => non-zero exit.
#
# Required env:
#   DATABRICKS_HOST       https://adb-xxxx.azuredatabricks.net
#   BENCH_WAREHOUSE_ID    SQL warehouse id — used to CREATE/DROP the per-point tables
#   DATABRICKS_TOKEN      PAT or Entra token (required unless BENCH_MINT_CMD is set, below)
# Optional:
#   BENCH_MINT_CMD   shell command printing a fresh token to stdout; re-run before EVERY point so the
#                    sweep never carries an about-to-expire token into a multi-minute run. e.g.:
#                      BENCH_MINT_CMD='az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken -o tsv'
#   BENCH_SCHEMA     catalog.schema the fresh tables are created in   (default main.default)
#   BENCH_BATCHES    space-separated batch sizes to sweep   (default "100000 250000 500000 1000000 2500000")
#   BENCH_SWEEP_ROWS rows written per point                 (default 5000000)
#   BENCH_TIMEOUT    per-point hard timeout, seconds        (default 1800)
#   OUT_CSV          results file (appended)                (default batch-sweep.csv)
#
# Needs: docker, curl. (No jq — the DDL carries no quotes, so the request JSON is built inline.)
set -uo pipefail

: "${DATABRICKS_HOST:?set DATABRICKS_HOST}"
: "${BENCH_WAREHOUSE_ID:?set BENCH_WAREHOUSE_ID (SQL warehouse for CREATE/DROP of per-point tables)}"
SCHEMA="${BENCH_SCHEMA:-main.default}"
BATCHES="${BENCH_BATCHES:-100000 250000 500000 1000000 2500000}"
ROWS="${BENCH_SWEEP_ROWS:-5000000}"
TIMEOUT="${BENCH_TIMEOUT:-1800}"
OUT_CSV="${OUT_CSV:-batch-sweep.csv}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOGDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/sweep-logs"
mkdir -p "$LOGDIR"
RUNTAG="$(date +%Y%m%d%H%M%S)"

mint() { # refresh DATABRICKS_TOKEN if a mint command was supplied; fail fast if it errors
  [ -n "${BENCH_MINT_CMD:-}" ] || { [ -n "${DATABRICKS_TOKEN:-}" ] || { echo "set DATABRICKS_TOKEN or BENCH_MINT_CMD" >&2; exit 1; }; return; }
  DATABRICKS_TOKEN="$(eval "$BENCH_MINT_CMD")" || { echo "[SWEEP] token mint failed" >&2; exit 1; }
  [ -n "$DATABRICKS_TOKEN" ] || { echo "[SWEEP] mint produced an empty token" >&2; exit 1; }
}

uc_sql() { # $1=SQL — run synchronously on the warehouse; fail loudly if not SUCCEEDED. (Tokens never echoed.)
  local resp
  resp="$(curl -sS -X POST "$DATABRICKS_HOST/api/2.0/sql/statements" \
    -H "Authorization: Bearer $DATABRICKS_TOKEN" -H "Content-Type: application/json" \
    -d "{\"statement\":\"$1\",\"warehouse_id\":\"$BENCH_WAREHOUSE_ID\",\"wait_timeout\":\"30s\"}")" || return 1
  case "$resp" in
    *'"state":"SUCCEEDED"'*) return 0 ;;
    *) echo "[SWEEP] SQL did not succeed: ${resp//$DATABRICKS_TOKEN/REDACTED}" >&2; return 1 ;;
  esac
}

ddl() { # envelope schema (mirrors bench-table.sql), single line for inline JSON
  echo "CREATE TABLE IF NOT EXISTS $1 (before STRUCT<id:INT,name:STRING,email:STRING>, after STRUCT<id:INT,name:STRING,email:STRING>, op STRING, ts_ms BIGINT, source STRUCT<db:STRING,table_name:STRING,lsn:BIGINT>) TBLPROPERTIES ('delta.feature.catalogManaged'='supported')"
}

[ -s "$OUT_CSV" ] || echo "batch,rows_per_sec,commits,p50_s,p99_s" > "$OUT_CSV"
failed=0
echo "[SWEEP] $RUNTAG | rows/point=$ROWS timeout=${TIMEOUT}s batches: $BATCHES"

for B in $BATCHES; do
  mint
  TABLE="${SCHEMA}.bench_sweep_${B}_${RUNTAG}"
  LOG="$LOGDIR/batch-${B}-${RUNTAG}.log"
  echo "[SWEEP] ===== batch=$B -> $TABLE ====="
  if ! uc_sql "$(ddl "$TABLE")"; then echo "[SWEEP] FAIL batch=$B: could not create table"; failed=1; continue; fi

  # stream live (tee) AND fail-fast (timeout); --init so the JVM gets SIGTERM/cleanup on timeout.
  timeout --signal=TERM "${TIMEOUT}" docker run --rm --init \
    -v "$ROOT/delta-sink-connector:/work" -v "${HOME}/.m2:/root/.m2" -w /work \
    -e DATABRICKS_HOST -e DATABRICKS_TOKEN \
    -e BENCH_TABLE="$TABLE" -e BENCH_ROWS="$ROWS" -e BENCH_BATCH="$B" \
    maven:3.9-eclipse-temurin-17 \
    mvn -B -Dtest=BenchmarkTest -Djacoco.skip=true -DargLine=-Xmx3g test 2>&1 | tee "$LOG"
  rc=${PIPESTATUS[0]}

  if [ "$rc" -eq 124 ]; then echo "[SWEEP] FAIL batch=$B: TIMED OUT after ${TIMEOUT}s (likely a commit storm) — see $LOG"; failed=1
  elif [ "$rc" -ne 0 ]; then echo "[SWEEP] FAIL batch=$B: exit $rc — see $LOG"; failed=1
  else
    line="$(grep -m1 '\[BENCH-RESULT\]' "$LOG" || true)"
    if [ -z "$line" ]; then echo "[SWEEP] FAIL batch=$B: build passed but no [BENCH-RESULT] — see $LOG"; failed=1
    else
      rps=$(sed -n 's/.*rows_per_sec=\([0-9]*\).*/\1/p' <<<"$line")
      com=$(sed -n 's/.*commits=\([0-9]*\).*/\1/p' <<<"$line")
      p50=$(sed -n 's/.*p50=\([0-9]*\).*/\1/p' <<<"$line")
      p99=$(sed -n 's/.*p99=\([0-9]*\).*/\1/p' <<<"$line")
      printf '%s,%s,%s,%.2f,%.2f\n' "$B" "$rps" "$com" "$(echo "$p50/1000"|bc -l)" "$(echo "$p99/1000"|bc -l)" >> "$OUT_CSV"
      echo "[SWEEP] ok batch=$B: ${rps} rows/s, ${com} commits, p50=${p50}ms p99=${p99}ms"
    fi
  fi
  uc_sql "DROP TABLE IF EXISTS $TABLE" || echo "[SWEEP] warn: drop of $TABLE failed (clean up by hand)"
done

echo "[SWEEP] done -> $OUT_CSV"; column -s, -t "$OUT_CSV" 2>/dev/null || cat "$OUT_CSV"
exit $failed
