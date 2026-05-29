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
}
