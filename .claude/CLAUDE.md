# kafka-connect-delta

Kafka Connect sink: Kafka/Warpstream topics -> Unity Catalog **managed** Delta tables via the Delta
Kernel Java API (no Spark). Append-only bronze. Design + live findings: `docs/SPEC.md`.

## build / test — in Docker, not on the host

`hadoop-azure` wants winutils/`HADOOP_HOME` on Windows and the offline Kernel tests hit the same wall,
so build on Linux. `Live*Test` is env-gated and skips without `DATABRICKS_HOST`, so this is offline and
deterministic:

    docker run --rm -v "<repo>/delta-sink-connector:/work" -v "$HOME/.m2:/root/.m2" \
      -w /work maven:3.9-eclipse-temurin-17 mvn -B test

From a Windows shell, run docker through PowerShell with native paths
(`-v "C:\...\delta-sink-connector:/work"`) — git-bash mangles the `:/work` mount and leaves a stray dir.

## live test (managed UC write)

Needs the Databricks "external writes to managed tables" Beta enabled and the table on
`delta.feature.catalogManaged=supported` (DBR 16.4+). Set `DATABRICKS_HOST`, `UC_TABLE`, and
`DATABRICKS_TOKEN` — a PAT, or an AAD token:

    az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken -o tsv

then add `-Dtest=LiveManagedUcWriteTest` to the docker command above.

## conventions

- Append-only bronze: no UPDATE/DELETE/MERGE in the connector (Kernel's write API is append-only). Do
  DML downstream in Databricks.
- Comments explain WHY, not WHAT; no javadoc that restates the signature. Terse, lowercase-leaning, no
  marketing words.
