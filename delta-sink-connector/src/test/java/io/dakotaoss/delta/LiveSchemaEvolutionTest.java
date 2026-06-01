package io.dakotaoss.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Live: with {@code schema.evolution=add}, a second flush whose records carry a new nullable column
 * evolves the catalog-managed table via {@code ALTER TABLE ADD COLUMNS} (on a SQL warehouse) and then
 * appends -- instead of DLQ-ing the wider rows. Proves the REST/DDL evolution path end to end (no
 * column mapping, which Kernel 4.2.0 cannot write). Skipped unless {@code DATABRICKS_HOST} is set.
 *
 * <p>Env: {@code DATABRICKS_HOST}, {@code DATABRICKS_TOKEN}, optional {@code UC_SCHEMA} (default
 * {@code teck_testing.cdc}) and {@code UC_WAREHOUSE}. The principal needs CREATE TABLE + EXTERNAL USE
 * SCHEMA on the schema.
 */
class LiveSchemaEvolutionTest {

  @Test
  @EnabledIfEnvironmentVariable(named = "DATABRICKS_HOST", matches = ".+")
  void additiveColumnEvolvesTableInsteadOfDlq() throws Exception {
    String host = System.getenv("DATABRICKS_HOST");
    String token = System.getenv("DATABRICKS_TOKEN");
    String schema = System.getenv().getOrDefault("UC_SCHEMA", "teck_testing.cdc");
    String fullName = schema + ".evolve_live_" + System.nanoTime();

    Map<String, String> props = new HashMap<>();
    props.put("name", "evolve-live");
    props.put(DeltaSinkConfig.WORKSPACE_URL, host);
    props.put(DeltaSinkConfig.TOKEN, token);
    props.put(DeltaSinkConfig.TABLE_NAME_FORMAT, fullName);
    props.put(DeltaSinkConfig.FLUSH_SIZE, "1"); // each put flushes immediately
    props.put(DeltaSinkConfig.AUTO_CREATE_TABLES, "true");
    props.put(DeltaSinkConfig.SCHEMA_EVOLUTION, DeltaSinkConfig.EVOLVE_ADD);
    props.put(
        DeltaSinkConfig.WAREHOUSE_ID,
        System.getenv().getOrDefault("UC_WAREHOUSE", "5bb094c3442e77bb"));

    Schema v1 =
        SchemaBuilder.struct()
            .name("row")
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    Schema v2 = // same shape + a new nullable column
        SchemaBuilder.struct()
            .name("row")
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("email", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    SinkRecord r1 =
        new SinkRecord("t", 0, null, null, v1, new Struct(v1).put("id", 1).put("name", "before"), 0L);
    SinkRecord r2 =
        new SinkRecord(
            "t", 0, null, null, v2,
            new Struct(v2).put("id", 2).put("name", "after").put("email", "e@x.example"), 1L);

    DeltaSinkTask task = new DeltaSinkTask();
    try {
      task.start(props);
      task.put(List.of(r1)); // auto-create (id, name) + append v1
      task.put(List.of(r2)); // diff sees +email -> ALTER ADD COLUMNS, reload, append v2
      Map<TopicPartition, OffsetAndMetadata> safe = task.preCommit(Map.of());
      assertEquals(
          2L, safe.get(new TopicPartition("t", 0)).offset(), "both writes committed; offset advances");

      // the table now carries the evolved column (proves the ALTER ran, not a DLQ)
      String meta = getTable(host, token, fullName);
      assertTrue(meta.contains("\"name\":\"email\""), "evolved column present in UC table metadata");
      System.out.println("[LIVE] evolved " + fullName + " (+email) and appended");
    } finally {
      task.stop();
      deleteTable(host, token, fullName);
    }
  }

  private static String getTable(String host, String token, String fullName) throws Exception {
    HttpRequest req =
        HttpRequest.newBuilder(URI.create(host + "/api/2.1/unity-catalog/tables/" + fullName))
            .header("Authorization", "Bearer " + token)
            .GET()
            .build();
    return HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofString()).body();
  }

  private static void deleteTable(String host, String token, String fullName) {
    try {
      HttpRequest req =
          HttpRequest.newBuilder(URI.create(host + "/api/2.1/unity-catalog/tables/" + fullName))
              .header("Authorization", "Bearer " + token)
              .DELETE()
              .build();
      HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofString());
    } catch (Exception ignore) {
      // best-effort cleanup
    }
  }
}
