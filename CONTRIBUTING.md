# Contributing

The connector lives in `delta-sink-connector/` (the Maven module). The repo root holds `docs/` and
`LICENSE`. Read [docs/SPEC.md](docs/SPEC.md) for the design and what the live runs flushed out before
touching the write path.

## Prerequisites

JDK 17 and Maven. Nothing else for the offline build.

If you don't want a local toolchain — or you're on Windows (see below) — build and test through Docker
instead; the image carries both:

```
docker run --rm -v "$PWD/delta-sink-connector:/work" -v "$HOME/.m2:/root/.m2" -w /work \
  maven:3.9-eclipse-temurin-17 mvn -B test
```

## Build and offline tests

From `delta-sink-connector/`:

```
mvn test
```

This compiles and runs the offline suite against a local `file://` Delta table — no Databricks, no
network. The live and benchmark tests are gated on environment variables (`@EnabledIfEnvironmentVariable`)
and auto-skip when those are unset, so `mvn test` is green on a clean checkout. Coverage report lands at
`target/site/jacoco/index.html`.

The offline suite covers the real Delta write protocol (create + append + read-back + idempotent
replay), the put/flush/`preCommit` cadence, schema mapping, record conversion, and the UC REST / ABFS
config builders. It can't cover catalog-commit coordination — that needs a live workspace with the Beta
feature, which is what the live tests are for.

## Windows

The Kernel default engine writes Parquet through Hadoop, and Hadoop's native filesystem code wants
`winutils.exe` + `HADOOP_HOME` on Windows. Rather than set that up, build and run through the Docker
image above (or any Linux box). The offline tests use `file://` and mostly work either way, but the
ABFS write path only exercises cleanly on Linux, and that's where the live runs happen.

## Live tests

The live tests prove the full managed-UC path — table resolve, READ_WRITE credential vending, ABFS
write with the vended SAS, and a UC-coordinated commit — against a real Databricks workspace. They're
the only thing that exercises catalog commits; the offline `file://` tests never load the ABFS stack.

### Workspace setup (once)

- DBR 16.4+.
- Enable the **External Access to UC Managed Delta Table** Beta preview for the workspace (Settings →
  Previews). Without it, READ_WRITE credential vending on a managed table is refused.
- A UC managed table created with the catalog-managed feature:
  ```sql
  CREATE TABLE main.default.delta_sink_smoke (id INT, name STRING, ts LONG)
  TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported');
  ```
- `GRANT EXTERNAL USE SCHEMA ON SCHEMA main.default TO \`<principal>\`;` and external data access
  enabled on the metastore. The token's principal needs this to vend credentials.

The flat and envelope tests need their own tables. See the `CREATE TABLE` blocks in
`LiveDebeziumFlatWriteTest` and `LiveDebeziumEnvelopeWriteTest`.

### Environment

| Var | Used by | Notes |
|---|---|---|
| `DATABRICKS_HOST` | all live tests | `https://adb-….azuredatabricks.net`. Setting it enables the live tests. |
| `DATABRICKS_TOKEN` | all live tests | PAT, or an Entra token (below). |
| `UC_TABLE` | `LiveManagedUcWriteTest` | full `catalog.schema.table`. |
| `DEBEZIUM_FLAT_TABLE` | `LiveDebeziumFlatWriteTest` | flattened CDC row (post-`ExtractNewRecordState`). |
| `DEBEZIUM_ENVELOPE_TABLE` | `LiveDebeziumEnvelopeWriteTest` | nested before/after/source envelope. |

`DATABRICKS_TOKEN` takes a PAT or a short-lived Entra access token:

```
az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken -o tsv
```

Entra/vended tokens expire in ~1h; for a long run, remint.

### Run

Docker is the path of least resistance (avoids the Windows Hadoop wall):

```
TOK=$(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken -o tsv)
docker run --rm -v "$PWD/delta-sink-connector:/work" -v "$HOME/.m2:/root/.m2" -w /work \
  -e DATABRICKS_HOST="https://adb-….azuredatabricks.net" \
  -e DATABRICKS_TOKEN="$TOK" \
  -e UC_TABLE="main.default.delta_sink_smoke" \
  maven:3.9-eclipse-temurin-17 mvn -B -Dtest=LiveManagedUcWriteTest test
```

A failed commit can leave an orphan uncommitted Parquet file under the table's storage path. Delta
ignores it; `VACUUM` removes it.

## Benchmarks

Results, charts, and raw CSVs are in `docs/benchmarks/` (`README.md` has the writeup). The harness is
`BenchmarkTest`, gated on `BENCH_ROWS` (plus `DATABRICKS_HOST`/`DATABRICKS_TOKEN`, `BENCH_TABLE`, and
optionally `BENCH_BATCH`). `BENCH_TABLE` is a catalog-managed table with the envelope schema.

Regenerate the charts from the CSVs:

```
uv run --with matplotlib --with numpy python docs/benchmarks/make_charts.py
```

## Code style

google-java-format compatible (2-space indent, ordered imports). Comments are terse and explain **why**,
not what — the SAS-provider wiring in `UcTableResolver` and the async-maintenance split in
`DeltaKernelWriter` are there because the obvious approach was wrong, and the comments say so. Skip
javadoc that restates a signature. No AI-padding.

## Pull requests

Small, focused PRs. Keep `mvn test` green; if a change touches the write path, run the relevant live
test before sending it (the offline suite can't see catalog commits). Match the surrounding style.
