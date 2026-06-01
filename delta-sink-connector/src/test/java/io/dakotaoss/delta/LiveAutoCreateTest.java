package io.dakotaoss.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.dakotaoss.delta.uc.UnityCatalogClient;
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
 * Live: the connector auto-creates an absent catalog-managed table on first write (auto.create.tables)
 * and then appends. Skipped unless {@code DATABRICKS_HOST} is set. Drives the real {@link DeltaSinkTask}
 * path (start -> put -> preCommit) against a unique table name, then deletes the table.
 *
 * <p>Env: {@code DATABRICKS_HOST}, {@code DATABRICKS_TOKEN}, optional {@code UC_SCHEMA} (default
 * {@code teck_testing.cdc}). The principal needs CREATE TABLE + EXTERNAL USE SCHEMA on the schema.
 */
class LiveAutoCreateTest {

  @Test
  @EnabledIfEnvironmentVariable(named = "DATABRICKS_HOST", matches = ".+")
  void autoCreatesCatalogManagedTableOnFirstWrite() throws Exception {
    String host = System.getenv("DATABRICKS_HOST");
    String token = System.getenv("DATABRICKS_TOKEN");
    String schema = System.getenv().getOrDefault("UC_SCHEMA", "teck_testing.cdc");
    String fullName = schema + ".autocreate_live_" + System.nanoTime();

    Map<String, String> props = new HashMap<>();
    props.put("name", "auto-create-live");
    props.put(DeltaSinkConfig.WORKSPACE_URL, host);
    props.put(DeltaSinkConfig.TOKEN, token);
    props.put(DeltaSinkConfig.TABLE_NAME_FORMAT, fullName); // literal 3-part name; ${topic} unused
    props.put(DeltaSinkConfig.FLUSH_SIZE, "1"); // flush on the first record
    props.put(DeltaSinkConfig.AUTO_CREATE_TABLES, "true");
    props.put(
        DeltaSinkConfig.WAREHOUSE_ID,
        System.getenv().getOrDefault("UC_WAREHOUSE", "5bb094c3442e77bb"));

    Schema value =
        SchemaBuilder.struct()
            .name("row")
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    SinkRecord rec =
        new SinkRecord(
            "orders", 0, null, null, value, new Struct(value).put("id", 1).put("name", "live-auto"), 0L);

    DeltaSinkTask task = new DeltaSinkTask();
    try {
      task.start(props);
      task.put(List.of(rec)); // first write: auto-create (v0) + append (v1)
      Map<TopicPartition, OffsetAndMetadata> safe = task.preCommit(Map.of());
      assertEquals(
          1L, safe.get(new TopicPartition("orders", 0)).offset(), "offset advances after commit");
      assertNotNull(
          new UnityCatalogClient(host, token).getTable(fullName).tableId,
          "auto-created table resolves in UC");
      System.out.println("[LIVE] auto-created + wrote " + fullName);
    } finally {
      task.stop();
      deleteTable(host, token, fullName);
    }
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
