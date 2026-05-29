package io.dakotaoss.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.dakotaoss.delta.uc.UcTableResolver;
import io.dakotaoss.delta.uc.UnityCatalogClient;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UnityCatalogClientTest {

  private HttpServer server;
  private String baseUrl;

  @BeforeEach
  void startServer() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/api/2.1/unity-catalog/tables/",
        ex ->
            respond(
                ex,
                200,
                "{\"table_id\":\"abc-123\","
                    + "\"storage_location\":\"abfss://cont@acct.dfs.core.windows.net/__unitystorage/t\"}"));
    server.createContext(
        "/api/2.1/unity-catalog/temporary-table-credentials",
        ex ->
            respond(
                ex,
                200,
                "{\"azure_user_delegation_sas\":{\"sas_token\":\"sig=abc&se=2026\"},"
                    + "\"url\":\"abfss://cont@acct.dfs.core.windows.net/__unitystorage/t\","
                    + "\"expiration_time\":1893456000000}"));
    server.createContext(
        "/api/2.1/unity-catalog/tables/denied",
        ex -> respond(ex, 403, "{\"error_code\":\"PERMISSION_DENIED\"}"));
    server.start();
    baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  private static void respond(HttpExchange ex, int code, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    ex.getResponseHeaders().add("Content-Type", "application/json");
    ex.sendResponseHeaders(code, bytes.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(bytes);
    }
  }

  @Test
  void resolvesTableAndVendsWriteCredentials() throws Exception {
    UnityCatalogClient client = new UnityCatalogClient(baseUrl, "test-token");
    UnityCatalogClient.TableInfo info = client.getTable("main.ingestion.orders");
    assertEquals("abc-123", info.tableId);
    assertTrue(info.storageLocation.startsWith("abfss://"));

    UnityCatalogClient.TemporaryCredentials creds =
        client.getTemporaryCredentials(info.tableId, "READ_WRITE");
    assertEquals("sig=abc&se=2026", creds.azureSasToken().orElseThrow());
    assertTrue(creds.expirationTimeMillis().isPresent());
  }

  @Test
  void buildsFixedSasAbfsConfig() throws Exception {
    UnityCatalogClient client = new UnityCatalogClient(baseUrl, "test-token");
    UnityCatalogClient.TableInfo info = client.getTable("main.ingestion.orders");
    UnityCatalogClient.TemporaryCredentials creds =
        client.getTemporaryCredentials(info.tableId, "READ_WRITE");
    Map<String, String> conf = UcTableResolver.abfsConfig(info.storageLocation, creds);
    String host = "acct.dfs.core.windows.net";
    assertEquals("SAS", conf.get("fs.azure.account.auth.type." + host));
    // provider type must stay unset: ABFS builds FixedSASTokenProvider from the fixed token itself,
    // and naming the class fails (no no-arg constructor)
    assertNull(conf.get("fs.azure.sas.token.provider.type." + host));
    assertEquals("sig=abc&se=2026", conf.get("fs.azure.sas.fixed.token." + host));
  }

  @Test
  void nonSuccessStatusRaisesIoException() {
    UnityCatalogClient client = new UnityCatalogClient(baseUrl, "test-token");
    IOException ex = assertThrows(IOException.class, () -> client.getTable("denied"));
    assertTrue(ex.getMessage().contains("403"));
  }
}
