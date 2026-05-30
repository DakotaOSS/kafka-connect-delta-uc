package io.dakotaoss.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class DeltaSinkConfigTest {

  private Map<String, String> required() {
    Map<String, String> p = new HashMap<>();
    p.put(DeltaSinkConfig.WORKSPACE_URL, "https://adb-1.azuredatabricks.net");
    p.put(DeltaSinkConfig.TOKEN, "secret");
    return p;
  }

  @Test
  void appliesDefaults() {
    DeltaSinkConfig c = new DeltaSinkConfig(required());
    assertEquals("main.ingestion.${topic}", c.tableNameFormat());
    assertEquals(500, c.flushSize());
    assertEquals(0L, c.flushBytes());
    assertEquals(5_000L, c.flushIntervalMs());
    assertTrue(c.partitionColumns().isEmpty());
    assertEquals("secret", c.token().value());
  }

  @Test
  void overridesAreRead() {
    Map<String, String> p = required();
    p.put(DeltaSinkConfig.TABLE_NAME_FORMAT, "cat.sch.${topic}");
    p.put(DeltaSinkConfig.FLUSH_SIZE, "10");
    p.put(DeltaSinkConfig.PARTITION_COLUMNS, "dt,region");
    DeltaSinkConfig c = new DeltaSinkConfig(p);
    assertEquals("cat.sch.${topic}", c.tableNameFormat());
    assertEquals(10, c.flushSize());
    assertEquals(2, c.partitionColumns().size());
  }

  @Test
  void missingRequiredFails() {
    assertThrows(ConfigException.class, () -> new DeltaSinkConfig(new HashMap<>()));
  }

  @Test
  void topicToTableParses() {
    Map<String, String> p = required();
    p.put(DeltaSinkConfig.TOPIC_TO_TABLE, "orders:main.sales.orders,users:analytics.cdc.users");
    Map<String, String> m = new DeltaSinkConfig(p).topicToTable();
    assertEquals("main.sales.orders", m.get("orders"));
    assertEquals("analytics.cdc.users", m.get("users"));
  }

  @Test
  void topicToTableEmptyByDefault() {
    assertTrue(new DeltaSinkConfig(required()).topicToTable().isEmpty());
  }

  @Test
  void topicToTableRejectsMalformedEntry() {
    Map<String, String> p = required();
    p.put(DeltaSinkConfig.TOPIC_TO_TABLE, "orders:main.sales"); // not a 3-part name
    assertThrows(ConfigException.class, () -> new DeltaSinkConfig(p));
  }

  @Test
  void topicToTableRejectsWhitespaceInsidePart() {
    Map<String, String> p = required();
    p.put(DeltaSinkConfig.TOPIC_TO_TABLE, "orders:main. sales .orders"); // space inside a part
    assertThrows(ConfigException.class, () -> new DeltaSinkConfig(p));
  }

  @Test
  void topicToTableRejectsDuplicateKey() {
    Map<String, String> p = required();
    p.put(DeltaSinkConfig.TOPIC_TO_TABLE, "orders:a.b.c,orders:d.e.f");
    assertThrows(ConfigException.class, () -> new DeltaSinkConfig(p));
  }
}
