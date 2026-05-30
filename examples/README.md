# examples — debezium → delta CDC pipeline

end-to-end CDC: SQL Server → Debezium → Kafka → this sink → append-only bronze Delta in Unity
Catalog → downstream MERGE for current state. two connector configs run on the same Connect worker.

```
SQL Server (CDC enabled)
   │  debezium-sqlserver-source.json  (io.debezium.connector.sqlserver.SqlServerConnector)
   ▼
Kafka topics  dakota.sales.dbo.customers, dakota.sales.dbo.orders   (Debezium envelope)
   │  delta-uc-sink.json  (io.dakotaoss.delta.DeltaSinkConnector)
   │    ExtractNewRecordState flattens envelope → after-image + op/lsn/ts_ms
   ▼
bronze Delta (UC managed, catalog-managed)  bronze.<db>.<table>      ── append-only, full change log
   │  MERGE / AUTO CDC in Databricks, ordered by lsn
   ▼
curated current-state tables  main.curated.*
```

bronze is the full change log, never mutated in place. the sink is append-only (Kernel write API has no
DML); deletes/updates land as rows and are resolved downstream. see
[../docs/SPEC.md](../docs/SPEC.md) for the commit protocol and merge templates.

## files

- `debezium-sqlserver-source.json` — Debezium SQL Server source. emits one topic per captured table,
  named `<topic.prefix>.<database>.<schema>.<table>` (here `dakota.sales.dbo.customers`, `dakota.sales.dbo.orders`).
- `delta-uc-sink.json` — this connector, consuming those topics. `ExtractNewRecordState` flattens the
  envelope; `table.name.format` routes each topic to a bronze table; the three flush dials set the
  commit cadence.

## the SMT and the columns it keeps

`transforms.unwrap.add.fields=op,source.lsn,source.ts_ms` flattens the envelope to the after-image and
re-adds three metadata fields. Debezium prefixes added fields with `__`, so the flattened value carries:

- `__op` — `c`/`u`/`d`/`r`
- `__source_lsn` — SQL Server LSN, the **monotonic source sequence**
- `__source_ts_ms` — source commit time

`delete.handling.mode=rewrite` turns deletes into a normal row with `__deleted=true` (and `__op=d`)
instead of a null value, so tombstones reach bronze as rows. `drop.tombstones=false` keeps the Kafka
tombstone too.

keep `__op` + a monotonic sequence (`__source_lsn`) on every bronze row — the downstream MERGE orders
and dedupes on the sequence, not arrival order. without it you cannot resolve out-of-order or duplicate
events.

## bronze table (create once, per topic)

the catalog-managed sink appends to a pre-created table; it does not create catalog-managed tables. the
table schema must match the flattened value, columns nullable (Kernel enforces nullability):

```sql
CREATE TABLE bronze.sales.customers (
  id INT, name STRING, email STRING,
  __op STRING, __source_lsn STRING, __source_ts_ms LONG, __deleted STRING
) TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported');
```

the example sets `table.name.format = "bronze.${topic[1]}.${topic[3]}"`, so topic
`dakota.sales.dbo.customers` (segments `dakota`/`sales`/`dbo`/`customers`) routes to
`bronze.sales.customers`, and `dakota.sales.dbo.orders` to `bronze.sales.orders` — one connector,
many tables, no per-topic config. to pin a topic to an arbitrary destination instead, add a
`topic.to.table` entry (`<topic>:<catalog>.<schema>.<table>`) that wins over the template. requires
the **External Access to UC Managed Delta Table** Beta and DBR 16.4+ (see
[../README.md](../README.md#status)).

## prerequisites

- the connector jar on the Connect worker plugin path. build it from `delta-sink-connector/`:
  ```
  docker run --rm -v "$PWD/delta-sink-connector:/work" -w /work maven:3.9-eclipse-temurin-17 mvn -B package
  ```
- Debezium SQL Server connector on the same worker.
- SQL Server with CDC enabled on the captured tables (`sys.sp_cdc_enable_table`), and an agent that can
  read the CDC tables.
- Databricks workspace: External Access to UC Managed Delta Table Beta enabled, DBR 16.4+, token
  principal granted `EXTERNAL USE SCHEMA` on `main.ingestion`, bronze tables created (above).
- secrets externalised via the worker's config provider. both configs read
  `${file:/opt/secrets/*.properties:key}`; wire `FileConfigProvider` (or your provider) on the worker.

## post the configs

source first (so the topics exist), then the sink:

```
curl -s -XPOST -H 'Content-Type: application/json' \
  http://localhost:8083/connectors -d @debezium-sqlserver-source.json

curl -s -XPOST -H 'Content-Type: application/json' \
  http://localhost:8083/connectors -d @delta-uc-sink.json
```

check status:

```
curl -s http://localhost:8083/connectors/delta-sink-cdc/status
```

## downstream MERGE

bronze → current state in Databricks, ordered by `__source_lsn`. last-write-wins, deletes applied:

```sql
MERGE INTO main.curated.customers t
USING (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY __source_lsn DESC) AS rn
    FROM bronze.sales.customers
  ) WHERE rn = 1
) s
ON t.id = s.id
WHEN MATCHED AND s.__op = 'd'                THEN DELETE
WHEN MATCHED AND s.__source_lsn > t.__source_lsn THEN UPDATE SET *
WHEN NOT MATCHED AND s.__op <> 'd'           THEN INSERT *;
```

the `__source_lsn >` guard makes replays no-ops, so bronze idempotency and merge idempotency compose.
declarative alternative (Lakeflow AUTO CDC, `sequence_by = __source_lsn`) is in
[../docs/SPEC.md](../docs/SPEC.md#downstream-merge-templates).
