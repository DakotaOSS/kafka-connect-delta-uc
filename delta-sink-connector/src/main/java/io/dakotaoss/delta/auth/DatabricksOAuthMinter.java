package io.dakotaoss.delta.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.function.Supplier;

/**
 * OAuth2 client-credentials against the workspace's own token endpoint
 * ({@code {workspaceUrl}/oidc/v1/token}, scope {@code all-apis}) — Databricks-native service-principal
 * auth, cloud-agnostic. The client secret is read per mint from a {@link Supplier} so no extra durable
 * copy is held.
 */
public final class DatabricksOAuthMinter implements TokenMinter {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String tokenUrl;
  private final String clientId;
  private final Supplier<String> clientSecret;
  private final HttpClient http;

  public DatabricksOAuthMinter(String workspaceUrl, String clientId, Supplier<String> clientSecret) {
    this(workspaceUrl, clientId, clientSecret,
        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(15)).build());
  }

  // visible for tests: inject an HttpClient pointed at a local stub.
  DatabricksOAuthMinter(String workspaceUrl, String clientId, Supplier<String> clientSecret, HttpClient http) {
    String base = workspaceUrl.endsWith("/") ? workspaceUrl.substring(0, workspaceUrl.length() - 1) : workspaceUrl;
    this.tokenUrl = base + "/oidc/v1/token";
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.http = http;
  }

  @Override
  public Minted mint() throws Exception {
    String basic =
        Base64.getEncoder()
            .encodeToString((clientId + ":" + clientSecret.get()).getBytes(StandardCharsets.UTF_8));
    HttpRequest req =
        HttpRequest.newBuilder(URI.create(tokenUrl))
            .timeout(Duration.ofSeconds(30))
            .header("Authorization", "Basic " + basic)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString("grant_type=client_credentials&scope=all-apis"))
            .build();
    return parse(http.send(req, HttpResponse.BodyHandlers.ofString()));
  }

  /**
   * Parse a standard OAuth2 token response ({@code access_token} + {@code expires_in}). Never include
   * the response body in an error: it carries the access token. Shared by {@link AzureEntraMinter},
   * which returns the same shape.
   */
  static Minted parse(HttpResponse<String> resp) throws IOException {
    if (resp.statusCode() / 100 != 2) {
      throw new IOException("OAuth token endpoint returned " + resp.statusCode());
    }
    JsonNode b = MAPPER.readTree(resp.body());
    String tok = b.path("access_token").asText(null);
    if (tok == null || tok.isEmpty()) {
      throw new IOException("OAuth token response had no access_token");
    }
    return new Minted(tok, b.path("expires_in").asLong(3600));
  }
}
