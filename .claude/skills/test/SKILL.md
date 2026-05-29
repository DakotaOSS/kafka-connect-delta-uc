---
name: test
description: Build the connector and run its JUnit suite in Docker (offline + deterministic by default; live Unity Catalog tests run when DATABRICKS_HOST/UC_TABLE are set). Use to verify a code change.
---

# test

hadoop-azure wants winutils/`HADOOP_HOME` on Windows and the offline Kernel tests hit the same wall, so
run in the maven Docker image. `Live*Test` skips unless its env is set. Run from the repo root.

## offline (no creds, deterministic)

PowerShell (native paths — git-bash mangles the `:/work` mount and leaves a stray dir):

    docker run --rm -v "${PWD}\delta-sink-connector:/work" -v "${env:USERPROFILE}\.m2:/root/.m2" `
      -w /work maven:3.9-eclipse-temurin-17 mvn -B test

bash/Linux:

    docker run --rm -v "$PWD/delta-sink-connector:/work" -v "$HOME/.m2:/root/.m2" \
      -w /work maven:3.9-eclipse-temurin-17 mvn -B test

Expect `BUILD SUCCESS`; the 3 `Live*Test` + `BenchmarkTest` skip without their env.

## live (managed UC write)

Needs the Databricks "external writes to managed tables" Beta on and the table at
`delta.feature.catalogManaged=supported` (DBR 16.4+). Add the env and target one test:

    ... -e DATABRICKS_HOST -e DATABRICKS_TOKEN -e UC_TABLE ... mvn -B -Dtest=LiveManagedUcWriteTest test

`DATABRICKS_TOKEN` is a PAT, or an Entra token:
`az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken -o tsv`.
