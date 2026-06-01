package io.dakotaoss.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.dakotaoss.delta.model.TableTarget;
import io.dakotaoss.delta.uc.UcTableResolver;
import io.dakotaoss.delta.uc.UnityCatalogClient;
import io.dakotaoss.delta.uc.VendedSasStore;
import io.dakotaoss.delta.uc.VendedSasTokenProvider;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

class UcTableResolverTest {

  private HttpServer serverWith(String tableJson, String credsJson) throws IOException {
    HttpServer s = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    s.createContext("/api/2.1/unity-catalog/tables/", ex -> respond(ex, tableJson));
    s.createContext(
        "/api/2.1/unity-catalog/temporary-table-credentials", ex -> respond(ex, credsJson));
    s.start();
    return s;
  }

  private static void respond(HttpExchange ex, String body) throws IOException {
    byte[] b = body.getBytes(StandardCharsets.UTF_8);
    ex.sendResponseHeaders(200, b.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(b);
    }
  }

  @Test
  void resolveFailureDropsRawCause() throws Exception {
    // resolve()'s catch must not chain the raw throwable: a future ABFS exception there could embed
    // a
    // SAS, and Connect logs the whole cause chain. Keep only the redacted message, no cause.
    HttpServer s = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    s.createContext(
        "/api/2.1/unity-catalog/tables/",
        ex -> {
          byte[] b = "{\"error_code\":\"INTERNAL\"}".getBytes(StandardCharsets.UTF_8);
          ex.sendResponseHeaders(500, b.length);
          try (OutputStream os = ex.getResponseBody()) {
            os.write(b);
          }
        });
    s.start();
    try {
      String base = "http://127.0.0.1:" + s.getAddress().getPort();
      UcTableResolver resolver =
          new UcTableResolver(
              new UnityCatalogClient(base, "tok"),
              "main.ingestion.${topic}",
              Collections.emptyList());
      ConnectException ex = assertThrows(ConnectException.class, () -> resolver.resolve("orders"));
      assertNull(ex.getCause(), "raw cause must be dropped so it can't be logged unredacted");
      assertTrue(ex.getMessage().contains("Failed to resolve UC table main.ingestion.orders"));
      assertFalse(ex.getMessage().contains("sig="), "any SAS in the cause must be redacted out");
    } finally {
      s.stop(0);
    }
  }

  @Test
  void resolvesTopicToTargetWithVendedAbfsConfig() throws Exception {
    HttpServer s =
        serverWith(
            "{\"table_id\":\"id-1\",\"storage_location\":\"abfss://c@acct.dfs.core.windows.net/t\"}",
            "{\"azure_user_delegation_sas\":{\"sas_token\":\"sig=zz\"}}");
    try {
      String base = "http://127.0.0.1:" + s.getAddress().getPort();
      UcTableResolver resolver =
          new UcTableResolver(
              new UnityCatalogClient(base, "tok"),
              "main.ingestion.${topic}",
              Collections.emptyList());
      TableTarget t = resolver.resolve("orders_v2");
      assertEquals("main.ingestion.orders_v2", t.fullName());
      assertEquals("abfss://c@acct.dfs.core.windows.net/t", t.tablePath());
      // the vended SAS is wired via our provider, not placed in the config
      assertEquals(
          VendedSasTokenProvider.class.getName(),
          t.hadoopConfig().get("fs.azure.sas.token.provider.type.acct.dfs.core.windows.net"));
      assertEquals(
          "sig=zz",
          VendedSasStore.instance().sasFor("acct.dfs.core.windows.net", "c", "t/_delta_log/0"));
    } finally {
      s.stop(0);
    }
  }

  @Test
  void nonAzureCredentialsYieldNoAbfsOverrides() throws Exception {
    HttpServer s =
        serverWith(
            "{\"table_id\":\"id-1\",\"storage_location\":\"s3://bucket/t\"}",
            "{\"aws_temp_credentials\":{\"access_key_id\":\"AK\"}}");
    try {
      String base = "http://127.0.0.1:" + s.getAddress().getPort();
      UcTableResolver resolver =
          new UcTableResolver(
              new UnityCatalogClient(base, "tok"),
              "main.ingestion.${topic}",
              Collections.emptyList());
      TableTarget t = resolver.resolve("events");
      assertTrue(t.hadoopConfig().isEmpty(), "non-azure creds should not set ABFS keys");
    } finally {
      s.stop(0);
    }
  }

  @Test
  void missingStorageLocationFailsFast() throws Exception {
    HttpServer s = serverWith("{\"table_id\":\"id-1\"}", "{}");
    try {
      String base = "http://127.0.0.1:" + s.getAddress().getPort();
      UcTableResolver resolver =
          new UcTableResolver(
              new UnityCatalogClient(base, "tok"),
              "main.ingestion.${topic}",
              Collections.emptyList());
      assertThrows(ConnectException.class, () -> resolver.resolve("orders"));
    } finally {
      s.stop(0);
    }
  }

  // ---- routing (resolveName: table.name.format tokens + topic.to.table overrides) --------------

  @Test
  void defaultTemplatePassesValidTopicThrough() {
    // a topic that is already a valid identifier substitutes unchanged
    assertEquals(
        "main.ingestion.orders_v2",
        UcTableResolver.resolveName(
            "main.ingestion.${topic}", Collections.emptyMap(), "orders_v2"));
    assertEquals(
        "main.ingestion.orders2",
        UcTableResolver.resolveName("main.ingestion.${topic}", Collections.emptyMap(), "orders2"));
  }

  @Test
  void rejectsOutOfSetCharactersInsteadOfCollapsing() {
    // orders.eu / orders/eu / orders-eu all used to fold to the same orders_eu (non-injective):
    // under
    // an untrusted regex subscription a crafted topic could collide onto a victim's table. Reject.
    for (String topic : new String[] {"orders-eu", "orders/eu", "orders.eu", "orders eu", "a$b"}) {
      assertThrows(
          ConnectException.class,
          () ->
              UcTableResolver.resolveName("main.ingestion.${topic}", Collections.emptyMap(), topic),
          "out-of-set topic must be rejected, not collapsed: " + topic);
    }
  }

  @Test
  void rejectsOversizedIdentifierPart() {
    String longTopic = new String(new char[300]).replace('\0', 'a'); // 300 chars, valid charset
    assertThrows(
        ConnectException.class,
        () ->
            UcTableResolver.resolveName(
                "main.ingestion.${topic}", Collections.emptyMap(), longTopic));
  }

  @Test
  void segmentTokensMapStructuredTopicToArbitraryName() {
    // bronze.<system>.<table> pulled from a dotted topic
    assertEquals(
        "bronze.sales.customers",
        UcTableResolver.resolveName(
            "bronze.${topic[0]}.${topic[2]}", Collections.emptyMap(), "sales.dbo.customers"));
    // fully topic-derived catalog.schema.table
    assertEquals(
        "sales.dbo.customers",
        UcTableResolver.resolveName(
            "${topic[0]}.${topic[1]}.${topic[2]}", Collections.emptyMap(), "sales.dbo.customers"));
  }

  @Test
  void explicitMapOverridesTemplateButFallsBackWhenUnmapped() {
    Map<String, String> map = new HashMap<>();
    map.put("orders", "analytics.cdc.orders");
    assertEquals(
        "analytics.cdc.orders",
        UcTableResolver.resolveName("main.ingestion.${topic}", map, "orders"));
    assertEquals(
        "main.ingestion.users",
        UcTableResolver.resolveName("main.ingestion.${topic}", map, "users"));
  }

  @Test
  void rejectsResultThatIsNotThreePartName() {
    assertThrows(
        ConnectException.class,
        () -> UcTableResolver.resolveName("only.two", Collections.emptyMap(), "t"));
  }

  @Test
  void rejectsOutOfRangeSegmentToken() {
    assertThrows(
        ConnectException.class,
        () -> UcTableResolver.resolveName("a.b.${topic[5]}", Collections.emptyMap(), "x.y"));
  }

  @Test
  void rejectsOversizedSegmentIndex() {
    // index > Integer.MAX_VALUE must surface as a clean ConnectException, not a raw
    // NumberFormatException
    assertThrows(
        ConnectException.class,
        () ->
            UcTableResolver.resolveName(
                "a.b.${topic[99999999999]}", Collections.emptyMap(), "x.y.z"));
  }

  @Test
  void rejectsEmptyRenderedSegment() {
    // empty leading topic segment -> empty catalog part
    assertThrows(
        ConnectException.class,
        () -> UcTableResolver.resolveName("${topic[0]}.s.t", Collections.emptyMap(), ".sales.x"));
    // adjacent dots in the format -> empty schema part
    assertThrows(
        ConnectException.class,
        () -> UcTableResolver.resolveName("main..${topic}", Collections.emptyMap(), "orders"));
  }

  @Test
  void routesFromRealConfigObject() {
    // wire the same two methods production uses (config -> 4-arg resolver), offline
    Map<String, String> props = new HashMap<>();
    props.put(DeltaSinkConfig.WORKSPACE_URL, "https://adb-1.azuredatabricks.net");
    props.put(DeltaSinkConfig.TOKEN, "t");
    props.put(DeltaSinkConfig.TABLE_NAME_FORMAT, "bronze.${topic[0]}.${topic[2]}");
    props.put(DeltaSinkConfig.TOPIC_TO_TABLE, "legacy:archive.raw.legacy");
    DeltaSinkConfig cfg = new DeltaSinkConfig(props);
    assertEquals(
        "archive.raw.legacy",
        UcTableResolver.resolveName(cfg.tableNameFormat(), cfg.topicToTable(), "legacy"));
    assertEquals(
        "bronze.sys.customers",
        UcTableResolver.resolveName(
            cfg.tableNameFormat(), cfg.topicToTable(), "sys.dbo.customers"));
  }
}
