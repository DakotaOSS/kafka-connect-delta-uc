package io.dakotaoss.delta.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OAuthMinterTest {

  private HttpServer server;
  private String baseUrl;
  private volatile String lastPath;
  private volatile String lastAuth;
  private volatile String lastBody;
  private volatile int status = 200;
  private volatile String responseBody =
      "{\"access_token\":\"minted-XYZ\",\"token_type\":\"Bearer\",\"expires_in\":3600}";

  @BeforeEach
  void start() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        ex -> {
          lastPath = ex.getRequestURI().getPath();
          lastAuth = ex.getRequestHeaders().getFirst("Authorization");
          lastBody = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
          respond(ex, status, responseBody);
        });
    server.start();
    baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
  }

  @AfterEach
  void stop() {
    server.stop(0);
  }

  private static void respond(HttpExchange ex, int code, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    ex.sendResponseHeaders(code, bytes.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(bytes);
    }
  }

  @Test
  void databricksClientCredentialsMintAndParse() throws Exception {
    DatabricksOAuthMinter m =
        new DatabricksOAuthMinter(baseUrl, "sp-id", () -> "sp-secret", HttpClient.newHttpClient());
    TokenMinter.Minted got = m.mint();
    assertEquals("minted-XYZ", got.token);
    assertEquals(3600, got.ttlSeconds);
    assertEquals("/oidc/v1/token", lastPath);
    assertTrue(lastBody.contains("grant_type=client_credentials"));
    assertTrue(lastBody.contains("scope=all-apis"));
    String expected =
        "Basic " + Base64.getEncoder().encodeToString("sp-id:sp-secret".getBytes(StandardCharsets.UTF_8));
    assertEquals(expected, lastAuth, "client id/secret go in HTTP Basic, not the body");
  }

  @Test
  void azureEntraClientCredentialsMintAndParse() throws Exception {
    AzureEntraMinter m =
        new AzureEntraMinter(baseUrl, "tenant-abc", "sp-id", () -> "sp-secret", HttpClient.newHttpClient());
    TokenMinter.Minted got = m.mint();
    assertEquals("minted-XYZ", got.token);
    assertEquals(3600, got.ttlSeconds);
    assertEquals("/tenant-abc/oauth2/v2.0/token", lastPath);
    assertTrue(lastBody.contains("grant_type=client_credentials"));
    assertTrue(lastBody.contains("client_id=sp-id"));
    assertTrue(lastBody.contains(AzureEntraMinter.DATABRICKS_RESOURCE), "scope targets the Databricks resource");
  }

  @Test
  void nonSuccessThrowsAndDoesNotLeakBody() {
    status = 401;
    responseBody = "{\"access_token\":\"should-not-appear\",\"error\":\"invalid_client\"}";
    DatabricksOAuthMinter m =
        new DatabricksOAuthMinter(baseUrl, "sp-id", () -> "sp-secret", HttpClient.newHttpClient());
    IOException e = assertThrows(IOException.class, m::mint);
    assertTrue(e.getMessage().contains("401"));
    assertFalse(e.getMessage().contains("should-not-appear"), "must not echo the token-bearing body");
  }
}
