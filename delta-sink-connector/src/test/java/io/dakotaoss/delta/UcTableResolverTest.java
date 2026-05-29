package io.dakotaoss.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.dakotaoss.delta.model.TableTarget;
import io.dakotaoss.delta.uc.UcTableResolver;
import io.dakotaoss.delta.uc.UnityCatalogClient;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

class UcTableResolverTest {

  private HttpServer serverWith(String tableJson, String credsJson) throws IOException {
    HttpServer s = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    s.createContext("/api/2.1/unity-catalog/tables/", ex -> respond(ex, tableJson));
    s.createContext("/api/2.1/unity-catalog/temporary-table-credentials", ex -> respond(ex, credsJson));
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
  void resolvesTopicToTargetWithVendedAbfsConfig() throws Exception {
    HttpServer s =
        serverWith(
            "{\"table_id\":\"id-1\",\"storage_location\":\"abfss://c@acct.dfs.core.windows.net/t\"}",
            "{\"azure_user_delegation_sas\":{\"sas_token\":\"sig=zz\"}}");
    try {
      String base = "http://127.0.0.1:" + s.getAddress().getPort();
      UcTableResolver resolver =
          new UcTableResolver(
              new UnityCatalogClient(base, "tok"), "main.ingestion.${topic}", Collections.emptyList());
      TableTarget t = resolver.resolve("orders-v2");
      assertEquals("main.ingestion.orders_v2", t.fullName());
      assertEquals("abfss://c@acct.dfs.core.windows.net/t", t.tablePath());
      assertEquals(
          "sig=zz", t.hadoopConfig().get("fs.azure.sas.fixed.token.acct.dfs.core.windows.net"));
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
              new UnityCatalogClient(base, "tok"), "main.ingestion.${topic}", Collections.emptyList());
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
              new UnityCatalogClient(base, "tok"), "main.ingestion.${topic}", Collections.emptyList());
      assertThrows(ConnectException.class, () -> resolver.resolve("orders"));
    } finally {
      s.stop(0);
    }
  }
}
