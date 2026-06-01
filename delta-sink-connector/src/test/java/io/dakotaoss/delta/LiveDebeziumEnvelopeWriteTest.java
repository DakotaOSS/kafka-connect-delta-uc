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
 * Live write for the full Debezium envelope shape: raw un-flattened CDC record with nested {@code
 * before}/{@code after}/{@code source} structs plus {@code op}/{@code ts_ms}. Exercises the
 * nested-struct path through {@link SchemaMapper} and {@link RecordConverter}, writing nested Delta
 * struct columns to a managed catalog-managed table.
 *
 * <p>Table must exist first (run once in Databricks):
 *
 * <pre>
 *   CREATE TABLE main.default.customers_envelope (
 *     before STRUCT&lt;id:INT,name:STRING,email:STRING&gt;,
 *     after  STRUCT&lt;id:INT,name:STRING,email:STRING&gt;,
 *     op STRING, ts_ms BIGINT,
 *     source STRUCT&lt;db:STRING,table_name:STRING,lsn:BIGINT&gt;)
 *   TBLPROPERTIES ('delta.feature.catalogManaged'='supported');
 * </pre>
 */
class LiveDebeziumEnvelopeWriteTest {

  @Test
  @EnabledIfEnvironmentVariable(named = "DATABRICKS_HOST", matches = ".+")
  void writesFullDebeziumEnvelopeToManagedTable() throws Exception {
    String host = System.getenv("DATABRICKS_HOST");
    String token = System.getenv("DATABRICKS_TOKEN");
    String fullName =
        System.getenv().getOrDefault("DEBEZIUM_ENVELOPE_TABLE", "main.default.customers_envelope");

    Schema row =
        SchemaBuilder.struct()
            .name("dakota.customers.Row")
            .optional()
            .field("id", Schema.OPTIONAL_INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("email", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    Schema source =
        SchemaBuilder.struct()
            .name("dakota.cdc.Source")
            .optional()
            .field("db", Schema.OPTIONAL_STRING_SCHEMA)
            .field("table_name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("lsn", Schema.OPTIONAL_INT64_SCHEMA)
            .build();
    Schema envelope =
        SchemaBuilder.struct()
            .name("dakota.customers.Envelope")
            .field("before", row)
            .field("after", row)
            .field("op", Schema.OPTIONAL_STRING_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
            .field("source", source)
            .build();

    long now = System.currentTimeMillis();
    List<SinkRecord> records = new ArrayList<>();
    // c: insert -> before null
    records.add(
        env(
            envelope,
            row,
            source,
            null,
            after(row, 101, "Acme Corp", "ops@acme.example"),
            "c",
            now,
            5001L,
            0));
    // u: update -> before + after
    records.add(
        env(
            envelope,
            row,
            source,
            after(row, 101, "Acme Corp", "ops@acme.example"),
            after(row, 101, "Acme Corporation", "ops@acme.example"),
            "u",
            now + 1,
            5002L,
            1));
    // d: delete -> after null
    records.add(
        env(
            envelope,
            row,
            source,
            after(row, 102, "Globex", "ar@globex.example"),
            null,
            "d",
            now + 2,
            5003L,
            2));

    StructType kernelSchema = SchemaMapper.toKernel(envelope);
    FilteredColumnarBatch batch = RecordConverter.toBatch(kernelSchema, envelope, records);

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

    DeltaKernelWriter w = new DeltaKernelWriter();
    io.delta.kernel.Snapshot snapshot =
        w.loadCatalogSnapshot(
            engine, target.tablePath(), committer, catalog.commits, catalog.maxVersion);
    io.delta.kernel.TransactionCommitResult result =
        w.appendToSnapshot(engine, snapshot, batch, "debezium-envelope", now);
    w.maintain(engine, result);

    assertTrue(result != null && result.getVersion() >= 1, "commit should produce a table version");
    System.out.println(
        "[LIVE] full Debezium envelope committed to "
            + fullName
            + " at version "
            + result.getVersion());
  }

  private static Struct after(Schema rowSchema, int id, String name, String email) {
    return new Struct(rowSchema).put("id", id).put("name", name).put("email", email);
  }

  private static SinkRecord env(
      Schema envelope,
      Schema rowSchema,
      Schema sourceSchema,
      Struct before,
      Struct after,
      String op,
      long tsMs,
      long lsn,
      long offset) {
    Struct src =
        new Struct(sourceSchema).put("db", "sales").put("table_name", "customers").put("lsn", lsn);
    Struct v =
        new Struct(envelope)
            .put("before", before)
            .put("after", after)
            .put("op", op)
            .put("ts_ms", tsMs)
            .put("source", src);
    return new SinkRecord("customers", 0, null, null, envelope, v, offset);
  }
}
