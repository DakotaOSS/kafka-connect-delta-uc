package io.dakotaoss.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DeltaSinkConnectorTest {

  private Map<String, String> props() {
    Map<String, String> p = new HashMap<>();
    p.put("name", "delta-sink");
    p.put(DeltaSinkConfig.WORKSPACE_URL, "https://adb-1.azuredatabricks.net");
    p.put(DeltaSinkConfig.TOKEN, "secret");
    return p;
  }

  @Test
  void producesOneConfigPerTask() {
    DeltaSinkConnector c = new DeltaSinkConnector();
    c.start(props());
    List<Map<String, String>> configs = c.taskConfigs(3);
    assertEquals(3, configs.size());
    assertEquals("secret", configs.get(0).get(DeltaSinkConfig.TOKEN));
    assertSame(DeltaSinkTask.class, c.taskClass());
    assertNotNull(c.config());
    assertEquals("0.1.0", c.version());
    c.stop();
  }
}
