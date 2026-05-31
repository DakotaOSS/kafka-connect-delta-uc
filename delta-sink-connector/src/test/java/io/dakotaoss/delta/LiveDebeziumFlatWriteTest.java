package io.dakotaoss.delta;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.dakotaoss.delta.model.TableTarget;
import io.dakotaoss.delta.schema.RecordConverter;
import io.dakotaoss.delta.schema.SchemaMapper;
import io.dakotaoss.delta.uc.UcTableResolver;
import io.dakotaoss.delta.uc.UnityCatalogClient;
import io.dakotaoss.delta.uc.UnityCatalogCommitter;
import io.dakotaoss.delta.writer.DeltaKernelWriter;
import io.dakotaoss.delta.writer.EngineProvider;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Live write for the flattened Debezium shape: a CDC record after the {@code ExtractNewRecordState}
 * SMT has unwrapped the envelope to the "after" image plus metadata columns ({@code op},
 * {@code source_ts_ms}, {@code lsn}). Flat struct, handled as-is by {@link SchemaMapper} /
 * {@link RecordConverter}, committed to a managed catalog-managed table via
 * {@link UnityCatalogCommitter}.
 *
 * <p>Table must exist first (run once in Databricks):
 * <pre>
 *   CREATE TABLE main.default.customers_flat
 *     (id INT, name STRING, email STRING, op STRING, source_ts_ms LONG, lsn LONG)
 *     TBLPROPERTIES ('delta.feature.catalogManaged'='supported');
 * </pre>
 */
class LiveDebeziumFlatWriteTest {

  @Test
  @EnabledIfEnvironmentVariable(named = "DATABRICKS_HOST", matches = ".+")
  void writesFlattenedDebeziumRowsToManagedTable() throws Exception {
    String host = System.getenv("DATABRICKS_HOST");
    String token = System.getenv("DATABRICKS_TOKEN");
    String fullName =
        System.getenv().getOrDefault("DEBEZIUM_FLAT_TABLE", "main.default.customers_flat");

    // value schema emitted after ExtractNewRecordState: flattened "after" + meta
    Schema value =
        SchemaBuilder.struct()
            .name("dakota.cdc.customers.Value")
            // must be optional: columns are nullable in SQL and Kernel enforces nullability
            .field("id", Schema.OPTIONAL_INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("email", Schema.OPTIONAL_STRING_SCHEMA)
            .field("op", Schema.OPTIONAL_STRING_SCHEMA)
            .field("source_ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
            .field("lsn", Schema.OPTIONAL_INT64_SCHEMA)
            .build();

    long now = System.currentTimeMillis();
    List<SinkRecord> records = new ArrayList<>();
    records.add(rec(value, 101, "Acme Corp", "ops@acme.example", "c", now, 1001L, 0));
    records.add(rec(value, 102, "Globex", "ar@globex.example", "c", now, 1002L, 1));
    // update for id=101 (op 'u'): appended to bronze, MERGEd downstream
    records.add(rec(value, 101, "Acme Corporation", "ops@acme.example", "u", now + 1, 1003L, 2));

    StructType kernelSchema = SchemaMapper.toKernel(value);
    FilteredColumnarBatch batch = RecordConverter.toBatch(kernelSchema, value, records);

    UnityCatalogClient uc = new UnityCatalogClient(host, token);
    UcTableResolver resolver = new UcTableResolver(uc, fullName, Collections.emptyList());
    TableTarget target = resolver.resolve("customers");
    Engine engine = EngineProvider.hadoop().engineFor(target);

    UnityCatalogClient.TableInfo info = uc.getTable(fullName);
    Configuration conf = new Configuration();
    target.hadoopConfig().forEach(conf::set);
    UnityCatalogCommitter committer =
        new UnityCatalogCommitter(host, token, info.tableId, info.storageLocation, conf);
    UnityCatalogCommitter.CatalogState catalog = committer.catalogState();

    DeltaKernelWriter.Result r =
        new DeltaKernelWriter()
            .appendCatalogManaged(
                engine,
                target.tablePath(),
                fullName,
                "debezium-flat",
                now,
                batch,
                committer,
                catalog.commits,
                catalog.maxVersion);

    assertTrue(r.version >= 1, "commit should produce a table version");
    System.out.println(
        "[LIVE] flattened Debezium rows committed to " + fullName + " at version " + r.version);
  }

  private static SinkRecord rec(
      Schema s, int id, String name, String email, String op, long tsMs, long lsn, long offset) {
    Struct v =
        new Struct(s)
            .put("id", id)
            .put("name", name)
            .put("email", email)
            .put("op", op)
            .put("source_ts_ms", tsMs)
            .put("lsn", lsn);
    return new SinkRecord("customers", 0, null, null, s, v, offset);
  }
}
