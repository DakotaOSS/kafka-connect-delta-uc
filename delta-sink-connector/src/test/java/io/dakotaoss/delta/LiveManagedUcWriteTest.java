package io.dakotaoss.delta;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.dakotaoss.delta.data.GenericColumnVector;
import io.dakotaoss.delta.data.GenericColumnarBatch;
import io.dakotaoss.delta.model.TableTarget;
import io.dakotaoss.delta.uc.UcTableResolver;
import io.dakotaoss.delta.uc.UnityCatalogClient;
import io.dakotaoss.delta.uc.UnityCatalogCommitter;
import io.dakotaoss.delta.writer.DeltaKernelWriter;
import io.dakotaoss.delta.writer.EngineProvider;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.util.Collections;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Live write against a real Databricks managed Unity Catalog table. Skipped unless the env is set,
 * so the offline build never runs it. Run by an agent or CI with Databricks access.
 *
 * <p>Required env:
 *
 * <ul>
 *   <li>{@code DATABRICKS_HOST} e.g. https://adb-123.4.azuredatabricks.net
 *   <li>{@code DATABRICKS_TOKEN} PAT/OAuth with EXTERNAL USE SCHEMA on the schema
 *   <li>{@code UC_TABLE} full name catalog.schema.table (managed, catalogManaged=supported)
 * </ul>
 *
 * <p>Table must exist first (run once in Databricks):
 *
 * <pre>
 *   CREATE TABLE {catalog}.{schema}.delta_sink_smoke (id INT, name STRING, ts LONG)
 *   TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported');
 * </pre>
 */
class LiveManagedUcWriteTest {

  @Test
  @EnabledIfEnvironmentVariable(named = "DATABRICKS_HOST", matches = ".+")
  void writesToManagedUnityCatalogTable() throws Exception {
    String host = System.getenv("DATABRICKS_HOST");
    String token = System.getenv("DATABRICKS_TOKEN");
    String fullName = System.getenv().getOrDefault("UC_TABLE", "main.ingestion.delta_sink_smoke");

    UnityCatalogClient uc = new UnityCatalogClient(host, token);
    // literal table name; no ${topic} substitution for the live test
    UcTableResolver resolver = new UcTableResolver(uc, fullName, Collections.emptyList());
    TableTarget target = resolver.resolve("ignored");
    Engine engine = EngineProvider.hadoop().engineFor(target);

    StructType schema =
        new StructType()
            .add("id", IntegerType.INTEGER, true)
            .add("name", StringType.STRING, true)
            .add("ts", LongType.LONG, true);

    long now = System.currentTimeMillis();
    ColumnVector[] cols =
        new ColumnVector[] {
          new GenericColumnVector(IntegerType.INTEGER, new Object[] {1, 2}),
          new GenericColumnVector(StringType.STRING, new Object[] {"live-a", "live-b"}),
          new GenericColumnVector(LongType.LONG, new Object[] {now, now}),
        };
    FilteredColumnarBatch batch =
        new FilteredColumnarBatch(new GenericColumnarBatch(schema, cols, 2), Optional.empty());

    // catalog-managed: commit goes through UC, not straight to the log
    UnityCatalogClient.TableInfo info = uc.getTable(fullName);
    Configuration conf = new Configuration();
    target.hadoopConfig().forEach(conf::set);
    UnityCatalogCommitter committer =
        new UnityCatalogCommitter(host, token, info.tableId, info.storageLocation, conf);
    UnityCatalogCommitter.CatalogState catalog = committer.catalogState();

    DeltaKernelWriter w = new DeltaKernelWriter();
    io.delta.kernel.Snapshot snapshot =
        w.loadCatalogSnapshot(
            engine, target.tablePath(), committer, catalog.commits, catalog.maxVersion);
    io.delta.kernel.TransactionCommitResult result =
        w.appendToSnapshot(engine, snapshot, batch, "live-smoke", now);
    w.maintain(engine, result);

    assertTrue(result != null && result.getVersion() >= 0, "commit should produce a table version");
    System.out.println("[LIVE] committed to " + fullName + " at version " + result.getVersion());
  }
}
