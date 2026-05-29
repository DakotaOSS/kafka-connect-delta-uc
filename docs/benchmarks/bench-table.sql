-- Catalog-managed target table for the benchmark (Debezium envelope shape).
-- Run once in Databricks SQL. Requires DBR 16.4+ and the "External Access to UC Managed Delta Table"
-- workspace preview enabled; the benchmark principal needs EXTERNAL USE SCHEMA on the schema.
CREATE TABLE IF NOT EXISTS main.default.bench_cdc (
  before STRUCT<id:INT, name:STRING, email:STRING>,
  after  STRUCT<id:INT, name:STRING, email:STRING>,
  op STRING,
  ts_ms BIGINT,
  source STRUCT<db:STRING, table_name:STRING, lsn:BIGINT>
) TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported');

-- GRANT EXTERNAL USE SCHEMA ON SCHEMA main.default TO `<principal>`;
