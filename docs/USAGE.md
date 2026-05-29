# Usage — deploy & operate

Deploy and run the Delta UC sink on a Kafka Connect worker. For the design and
the commit protocol see [SPEC.md](SPEC.md); for accurate config keys see
`delta-sink-connector/src/main/java/io/dakotaoss/delta/DeltaSinkConfig.java`.

The connector is an append-only bronze writer. It commits Debezium/Kafka records
into Unity Catalog **managed, catalog-managed** Delta tables via the Delta Kernel
Java API, with commits coordinated through UC and storage credentials vended per
table. No Spark, no second compute plane.

Connector class: `io.dakotaoss.delta.DeltaSinkConnector`.

## Prerequisites (UC side)

The managed-UC write path is gated on a Databricks Beta plus a grant. Confirm all
of these before deploying — a miss surfaces as a 403 at credential-vending time
(see [Troubleshooting](#troubleshooting)).

- **External Access to UC Managed Delta Table** Beta preview enabled for the
  workspace (Settings → Previews). A workspace admin must toggle it. Without it,
  READ_WRITE credential vending on a managed table is refused.
- **DBR 16.4+** — required to read/write/create catalog-commit tables.
- **External data access enabled on the metastore.**
- `GRANT EXTERNAL USE SCHEMA ON SCHEMA <catalog>.<schema> TO \`<principal>\`;`
  where `<principal>` owns the `databricks.token`. This is what lets the token
  vend credentials.
- Target table created with the **catalogManaged** feature:

  ```sql
  CREATE TABLE main.ingestion.orders (...)
  TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported');
  ```

  The connector appends to a pre-created catalog-managed table. It does not create
  catalog-managed tables; `partition.columns` applies only on the filesystem
  create path.

Azure/ADLS Gen2 (`abfss://`) only for the live path.

## Package the jar

Maven module is `delta-sink-connector/`. The build pins Kernel 4.2.0 and the UC
commit client; output is `target/kafka-connect-delta-uc-0.1.0-SNAPSHOT.jar`.

Docker (no local JDK; also avoids the Windows Hadoop/winutils wall):

```bash
docker run --rm -v "$PWD/delta-sink-connector:/work" -w /work \
  maven:3.9-eclipse-temurin-17 mvn -B package
```

Or Maven on JDK 17:

```bash
cd delta-sink-connector && mvn -B package
```

The build produces a thin jar — Kernel, `delta-storage`, the UC client,
`hadoop-azure`, and the unshaded `commons-lang3`/`commons-io` are runtime
dependencies, not bundled. A Connect plugin must be a self-contained directory, so
copy the connector jar **plus its runtime dependencies** into one folder:

```bash
cd delta-sink-connector
mvn -B package
mvn -B dependency:copy-dependencies \
  -DincludeScope=runtime \
  -DoutputDirectory=target/plugin
cp target/kafka-connect-delta-uc-*.jar target/plugin/
```

`target/plugin/` is the plugin directory. `connect-api` and `slf4j-api` are
`provided` (the Connect runtime supplies them) and are correctly excluded.

## Install on a Connect worker

Connect discovers plugins by directory under `plugin.path` (worker properties).
Put the plugin directory beside your other connectors:

```
/opt/kafka/connectors/
  kafka-connect-delta-uc/      <- target/plugin/ contents
  debezium-connector-sqlserver/
```

```properties
plugin.path=/opt/kafka/connectors
```

Restart the worker (or roll the cluster). Confirm the plugin is loaded:

```bash
curl -s localhost:8083/connector-plugins | \
  grep io.dakotaoss.delta.DeltaSinkConnector
```

## Externalize the token

`databricks.token` is a secret. Do not inline it in the connector config — use a
Connect **config provider** so it resolves at runtime and never lands in the REST
API or in connector status.

Worker properties — register a file provider:

```properties
config.providers=file
config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
```

Secret file on the worker (`/opt/secrets/databricks.properties`), readable only by
the Connect user:

```properties
token=dapi...        # PAT, or an Entra/AAD access token
```

Reference it in the connector config:

```json
"databricks.token": "${file:/opt/secrets/databricks.properties:token}"
```

Other providers work the same way (e.g. a Vault/secrets-manager provider via its
own `config.providers.*` class). The token principal needs `EXTERNAL USE SCHEMA`
on the target schema.

Entra/AAD tokens (and vended SAS) expire in ~1h. For long-running tasks, supply a
PAT or remint the token — credential refresh is reactive today (a flush failure
re-vends), proactive refresh is future work.

## Connector config

Verified keys (`DeltaSinkConfig`). Defaults shown.

| key | default | notes |
|---|---|---|
| `databricks.workspace.url` | — | UC REST base URL, e.g. `https://adb-1234567890.1.azuredatabricks.net` |
| `databricks.token` | — | bearer token (PAT/OAuth/AAD); externalize via config provider |
| `table.name.format` | `main.ingestion.${topic}` | 3-part UC name; `${topic}` substituted with the sanitised topic (`[^A-Za-z0-9_]`→`_`) |
| `partition.columns` | (none) | partition cols, used only when this connector creates a new table |
| `flush.size` | `500` | rows buffered per partition before commit; `0` disables the row dial |
| `flush.bytes` | `0` | approx buffered bytes before commit, for target file size (e.g. `134217728` = 128 MiB); `0` disables |
| `flush.interval.ms` | `5000` | max ms to buffer a partition before commit; the max-latency SLA |

Route **one task per table** (topic→table) so writers do not collide — set
`tasks.max` to match and keep `topics` aligned to one table per connector. UC
conflict arbitration is the safety net, not the primary concurrency strategy.

Tuning: the three flush dials trip independently, whichever first. `flush.size` /
`flush.bytes` flush opportunistically inside `put` when a buffer fills;
`flush.interval.ms` is enforced by a scheduler so queued rows commit within the
interval under light traffic. Raise `flush.bytes` toward 128–256 MiB to amortize
fixed per-commit cost and cut file count — at the cost of higher latency and
per-commit memory.

Full config example:

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
    "flush.interval.ms": "5000",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.add.fields": "op,source.lsn,source.ts_ms"
  }
}
```

POST it:

```bash
curl -s -X POST localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d @delta-sink-orders.json
```

## Debezium SMT chain

The connector writes append-only bronze. The Kernel write API has no DML, so
deletes/updates are not applied here — carry them as change events and merge
downstream in Databricks.

Flatten the Debezium envelope with `ExtractNewRecordState`, but keep the fields a
downstream MERGE needs — `op` and a **monotonic** source sequence (LSN for SQL
Server/Postgres, SCN for Oracle, commit-ts otherwise):

```json
"transforms": "unwrap",
"transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
"transforms.unwrap.drop.tombstones": "false",
"transforms.unwrap.delete.handling.mode": "rewrite",
"transforms.unwrap.add.fields": "op,source.lsn,source.ts_ms"
```

`drop.tombstones=false` + `delete.handling.mode=rewrite` keep deletes flowing as
rows (with `op='d'` and the before-image PK) instead of dropping them. The
downstream MERGE dedupes and orders by the sequence
(`ROW_NUMBER() OVER (PARTITION BY pk ORDER BY seq DESC)`); without a monotonic
sequence on every bronze row you cannot resolve out-of-order or duplicate events.
Merge templates: [SPEC.md](SPEC.md#downstream-merge-templates).

The SMT is on the **source** connector, or on this sink — either side flattens the
same envelope. Without the SMT, the full nested envelope also ingests:
`before`/`after`/`source` map to nested Delta struct columns. Flattening is
recommended because it makes the downstream MERGE simpler and `op`/`seq`
first-class.

## Monitoring

Two surfaces: the Delta table history (did the commit land, what did it write) and
Connect task metrics (is the task healthy, keeping up).

**Delta history.** `commitInfo` is enriched so the history reads like a
Spark-written table:

```sql
DESCRIBE HISTORY main.ingestion.orders;
```

Watch `operationMetrics.numOutputRows` / `numFiles` / `numOutputBytes` per commit,
`operationParameters.mode=Append`, and `isBlindAppend=true`. Commit cadence should
track the flush dials (one commit per flush per partition); a growing gap between
the latest Kafka offset and the latest committed version means the task is behind.
`numFiles` per commit climbing with small `numOutputBytes` means small files —
raise `flush.bytes`.

**Connect task metrics** (JMX / REST). Status:

```bash
curl -s localhost:8083/connectors/delta-sink-orders/status
```

Track the sink task MBeans (`kafka.connect:type=sink-task-metrics,...`):
`sink-record-read-rate`, `sink-record-send-rate`, `put-batch-avg-time-ms`,
`offset-commit-*`, and `partition-count`. A rising consumer lag on the sink's
group, or `offset-commit` latency tracking commit latency, indicates the write
path is the bottleneck — tune the flush dials or co-locate the worker with the
workspace region (cross-region adds WAN latency per commit).

## Troubleshooting

**403 on credential vending / ABFS write.** Most common failure. Check, in order:

- Beta **not enabled** — the **External Access to UC Managed Delta Table** preview
  is off for the workspace. READ_WRITE vending on a managed table is refused
  without it. Enable it (Settings → Previews) and confirm DBR 16.4+.
- Missing grant — the token principal lacks `EXTERNAL USE SCHEMA` on the target
  schema, or external data access is disabled on the metastore.
- Expired token — Entra/AAD tokens and vended SAS last ~1h. Remint and restart.
- ABFS 403 mid-write after vending succeeded — the vended SAS is table-dir-scoped;
  an HNS probe on the container root (outside scope) 403s. The connector sets
  `fs.azure.account.hns.enabled=true` to skip it. If you see this, confirm you are
  on the shipped tree (this was a fixed bug — see [SPEC.md](SPEC.md#live-test-findings-bugs-fixed)).

**Schema mismatch / commit rejected.** UC validates the schema at commit. Additive
evolution (new nullable columns) flows through; breaking changes (type change,
dropped/renamed column, nullability tightening) are rejected at commit and the
flush fails. Reconcile the Connect record schema with the table, or evolve the
table first. DLQ/retry routing for rejected commits is not yet built — a rejected
commit fails the task. Top-level `ARRAY`/`MAP` columns are rejected at schema-map
time (only nested `STRUCT` is supported); restructure or drop them in an SMT.

**Beta not enabled** — see the 403 case above; this is the symptom, not a separate
error. The failure is a refused credential-vend, surfaced as a 403 from
`POST /api/2.1/unity-catalog/temporary-table-credentials`.

**Orphan Parquet files.** A failed commit can leave an uncommitted Parquet file
under the table's storage path. Delta ignores uncommitted files; `VACUUM` removes
them.

**Wrong table name.** `table.name.format` must resolve to an existing 3-part UC
name (`catalog.schema.table`). `${topic}` is sanitised (`[^A-Za-z0-9_]`→`_`) before
substitution, so `orders.public` becomes `orders_public` — name the table to match.

See [SPEC.md](SPEC.md) for the full design, decisions, and known gaps.
