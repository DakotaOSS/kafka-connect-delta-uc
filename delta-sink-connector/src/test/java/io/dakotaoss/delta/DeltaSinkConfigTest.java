package io.dakotaoss.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.dakotaoss.delta.auth.Credentials;
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
  void schemaEvolutionDefaultsToNone() {
    assertEquals(DeltaSinkConfig.EVOLVE_NONE, new DeltaSinkConfig(required()).schemaEvolution());
  }

  @Test
  void schemaEvolutionAddRequiresAWarehouse() {
    Map<String, String> p = required();
    p.put(DeltaSinkConfig.SCHEMA_EVOLUTION, DeltaSinkConfig.EVOLVE_ADD);
    assertThrows(ConfigException.class, () -> new DeltaSinkConfig(p)); // no warehouse set
    p.put(DeltaSinkConfig.WAREHOUSE_ID, "wh-1");
    assertEquals(DeltaSinkConfig.EVOLVE_ADD, new DeltaSinkConfig(p).schemaEvolution());
  }

  @Test
  void schemaEvolutionRejectsUnknownPolicy() {
    Map<String, String> p = required();
    p.put(DeltaSinkConfig.SCHEMA_EVOLUTION, "rename");
    assertThrows(ConfigException.class, () -> new DeltaSinkConfig(p));
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

  @Test
  void authTypeDefaultsToPat() {
    assertEquals(DeltaSinkConfig.AUTH_PAT, new DeltaSinkConfig(required()).authType());
  }

  @Test
  void patRequiresToken() {
    Map<String, String> p = new HashMap<>();
    p.put(DeltaSinkConfig.WORKSPACE_URL, "https://adb-1.azuredatabricks.net"); // no token, default pat
    assertThrows(ConfigException.class, () -> new DeltaSinkConfig(p));
  }

  @Test
  void rejectsUnknownAuthType() {
    Map<String, String> p = required();
    p.put(DeltaSinkConfig.AUTH_TYPE, "kerberos");
    assertThrows(ConfigException.class, () -> new DeltaSinkConfig(p));
  }

  @Test
  void oauthM2mRequiresClientIdAndSecret() {
    Map<String, String> p = new HashMap<>();
    p.put(DeltaSinkConfig.WORKSPACE_URL, "https://adb-1.azuredatabricks.net");
    p.put(DeltaSinkConfig.AUTH_TYPE, DeltaSinkConfig.AUTH_OAUTH_M2M);
    p.put(DeltaSinkConfig.CLIENT_ID, "sp-id"); // secret still missing
    assertThrows(ConfigException.class, () -> new DeltaSinkConfig(p));
  }

  @Test
  void oauthM2mValidWithClientCredentials() {
    Map<String, String> p = new HashMap<>();
    p.put(DeltaSinkConfig.WORKSPACE_URL, "https://adb-1.azuredatabricks.net");
    p.put(DeltaSinkConfig.AUTH_TYPE, DeltaSinkConfig.AUTH_OAUTH_M2M);
    p.put(DeltaSinkConfig.CLIENT_ID, "sp-id");
    p.put(DeltaSinkConfig.CLIENT_SECRET, "sp-secret");
    DeltaSinkConfig c = new DeltaSinkConfig(p); // no token needed
    assertEquals(DeltaSinkConfig.AUTH_OAUTH_M2M, c.authType());
    assertEquals("sp-id", c.clientId());
    assertEquals("sp-secret", c.clientSecret().value());
  }

  @Test
  void azureEntraRequiresTenant() {
    Map<String, String> p = new HashMap<>();
    p.put(DeltaSinkConfig.WORKSPACE_URL, "https://adb-1.azuredatabricks.net");
    p.put(DeltaSinkConfig.AUTH_TYPE, DeltaSinkConfig.AUTH_AZURE_ENTRA);
    p.put(DeltaSinkConfig.CLIENT_ID, "sp-id");
    p.put(DeltaSinkConfig.CLIENT_SECRET, "sp-secret"); // tenant still missing
    assertThrows(ConfigException.class, () -> new DeltaSinkConfig(p));
  }

  @Test
  void patFactoryReturnsConfiguredToken() {
    // end-to-end: pat config -> StaticCredential that yields the configured token
    assertEquals("secret", Credentials.fromConfig(new DeltaSinkConfig(required())).get());
  }
}
