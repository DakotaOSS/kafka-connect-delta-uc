package io.dakotaoss.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Live proof that a single connector config routes several topics to distinct managed tables in one
 * run, using both {@code ${topic[N]}} segment tokens and explicit {@code topic.to.table} overrides.
 * Skipped unless {@code DATABRICKS_HOST} is set. Reuses the smoke/Debezium tables
 * ({@code UC_TABLE} / {@code DEBEZIUM_FLAT_TABLE} / {@code DEBEZIUM_ENVELOPE_TABLE}).
 */
class LiveMultiTableRoutingTest {

  @Test
  @EnabledIfEnvironmentVariable(named = "DATABRICKS_HOST", matches = ".+")
  void routesManyTopicsToDistinctTablesInOneConfig() throws Exception {
    String host = System.getenv("DATABRICKS_HOST");
    String token = System.getenv("DATABRICKS_TOKEN");
    String smoke = System.getenv().getOrDefault("UC_TABLE", "main.ingestion.delta_sink_smoke");
    String flat = System.getenv().getOrDefault("DEBEZIUM_FLAT_TABLE", "main.default.customers_flat");
    String envelope =
        System.getenv().getOrDefault("DEBEZIUM_ENVELOPE_TABLE", "main.default.customers_envelope");

    // One resolver, two routing mechanisms:
    //  - the smoke table is reached by a segment template, feeding the table's own dotted name as
    //    the topic (catalog.schema.table -> ${topic[0]}.${topic[1]}.${topic[2]});
    //  - flat + envelope are reached by explicit topic.to.table overrides.
    String smokeTopic = smoke;
    Map<String, String> overrides = new HashMap<>();
    overrides.put("feed.flat", flat);
    overrides.put("feed.envelope", envelope);

    UnityCatalogClient uc = new UnityCatalogClient(host, token);
    UcTableResolver resolver =
        new UcTableResolver(
            uc, "${topic[0]}.${topic[1]}.${topic[2]}", overrides, Collections.emptyList());

    // routing resolves to the three distinct tables (template for smoke, map for the others)
    assertEquals(smoke, resolver.resolve(smokeTopic).fullName());
    assertEquals(flat, resolver.resolve("feed.flat").fullName());
    assertEquals(envelope, resolver.resolve("feed.envelope").fullName());

    // and the vended creds actually write — two distinct schemas in the same run
    long v1 = write(host, token, resolver.resolve(smokeTopic), smokeBatch(), "route-smoke");
    long v2 = write(host, token, resolver.resolve("feed.flat"), flatBatch(), "route-flat");
    assertTrue(v1 >= 0 && v2 >= 1, "both routed commits should land");
    System.out.println("[LIVE] routed -> " + smoke + " @v" + v1 + " and " + flat + " @v" + v2);
  }

  private static long write(
      String host, String token, TableTarget target, FilteredColumnarBatch batch, String appId)
      throws Exception {
    Engine engine = EngineProvider.hadoop().engineFor(target);
    Configuration conf = new Configuration();
    target.hadoopConfig().forEach(conf::set);
    UnityCatalogCommitter committer =
        new UnityCatalogCommitter(host, token, target.tableId(), target.tablePath(), conf);
    UnityCatalogCommitter.CatalogState cs = committer.catalogState();
    return new DeltaKernelWriter()
        .appendCatalogManaged(
            engine, target.tablePath(), appId, System.currentTimeMillis(), batch, committer,
            cs.commits, cs.maxVersion)
        .version;
  }

  private static FilteredColumnarBatch smokeBatch() {
    Schema s =
        SchemaBuilder.struct()
            .field("id", Schema.OPTIONAL_INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("ts", Schema.OPTIONAL_INT64_SCHEMA)
            .build();
    List<SinkRecord> recs = new ArrayList<>();
    recs.add(
        new SinkRecord(
            "t", 0, null, null, s,
            new Struct(s).put("id", 1).put("name", "route-a").put("ts", System.currentTimeMillis()),
            0));
    return RecordConverter.toBatch(SchemaMapper.toKernel(s), s, recs);
  }

  private static FilteredColumnarBatch flatBatch() {
    Schema s =
        SchemaBuilder.struct()
            .field("id", Schema.OPTIONAL_INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("email", Schema.OPTIONAL_STRING_SCHEMA)
            .field("op", Schema.OPTIONAL_STRING_SCHEMA)
            .field("source_ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
            .field("lsn", Schema.OPTIONAL_INT64_SCHEMA)
            .build();
    List<SinkRecord> recs = new ArrayList<>();
    recs.add(
        new SinkRecord(
            "t", 0, null, null, s,
            new Struct(s)
                .put("id", 201)
                .put("name", "Routed Co")
                .put("email", "r@x.example")
                .put("op", "c")
                .put("source_ts_ms", System.currentTimeMillis())
                .put("lsn", 9001L),
            0));
    return RecordConverter.toBatch(SchemaMapper.toKernel(s), s, recs);
  }
}
