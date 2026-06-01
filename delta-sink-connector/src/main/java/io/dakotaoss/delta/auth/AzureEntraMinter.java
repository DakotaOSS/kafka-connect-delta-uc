package io.dakotaoss.delta.auth;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.function.Supplier;

/**
 * OAuth2 client-credentials against Microsoft Entra for the Azure Databricks resource, for an
 * Azure-hosted service principal. Same identity model as {@code az account get-access-token
 * --resource 2ff814a6-...}. Response shape matches Databricks OIDC, so parsing is shared with
 * {@link DatabricksOAuthMinter}.
 */
public final class AzureEntraMinter implements TokenMinter {

  // the Azure Databricks first-party application id; the SP token must target this resource.
  static final String DATABRICKS_RESOURCE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d";

  private final String tokenUrl;
  private final String clientId;
  private final Supplier<String> clientSecret;
  private final HttpClient http;

  public AzureEntraMinter(String tenantId, String clientId, Supplier<String> clientSecret) {
    this("https://login.microsoftonline.com", tenantId, clientId, clientSecret,
        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(15)).build());
  }

  // visible for tests: inject the authority base + HttpClient pointed at a local stub.
  AzureEntraMinter(
      String authorityBase, String tenantId, String clientId, Supplier<String> clientSecret, HttpClient http) {
    String base = authorityBase.endsWith("/") ? authorityBase.substring(0, authorityBase.length() - 1) : authorityBase;
    this.tokenUrl = base + "/" + tenantId + "/oauth2/v2.0/token";
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.http = http;
  }

  @Override
  public Minted mint() throws Exception {
    String form =
        "grant_type=client_credentials"
            + "&client_id=" + enc(clientId)
            + "&client_secret=" + enc(clientSecret.get())
            + "&scope=" + enc(DATABRICKS_RESOURCE + "/.default");
    HttpRequest req =
        HttpRequest.newBuilder(URI.create(tokenUrl))
            .timeout(Duration.ofSeconds(30))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(form))
            .build();
    return DatabricksOAuthMinter.parse(http.send(req, HttpResponse.BodyHandlers.ofString()));
  }

  private static String enc(String s) {
    return URLEncoder.encode(s, StandardCharsets.UTF_8);
  }
}
