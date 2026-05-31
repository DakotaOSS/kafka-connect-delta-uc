package io.dakotaoss.delta.uc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dakotaoss.delta.util.Redact;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Minimal client for the Unity Catalog REST API used by an external Delta writer:
 *
 * <ul>
 *   <li>{@code GET  /api/2.1/unity-catalog/tables/{full_name}} &mdash; resolve table id + storage
 *       location;</li>
 *   <li>{@code POST /api/2.1/unity-catalog/temporary-table-credentials} &mdash; vend short-lived
 *       READ_WRITE credentials (the token + storage URL the engine uses to write).</li>
 * </ul>
 *
 * <p>Requires the calling principal to hold {@code EXTERNAL USE SCHEMA} on the schema and the
 * metastore to have external data access enabled. The base URL is injectable so tests can point at a
 * local stub server.
 */
public final class UnityCatalogClient {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String baseUrl; // e.g. https://adb-123.4.azuredatabricks.net
  // sourced on each request, not held as a long-lived String, so refresh is picked up and we keep no
  // extra durable copy of the secret (the materialized String lives only for the request).
  private final Supplier<String> bearerToken;
  private final HttpClient http;

  public UnityCatalogClient(String baseUrl, Supplier<String> bearerToken) {
    this(baseUrl, bearerToken, HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(15)).build());
  }

  public UnityCatalogClient(String baseUrl, Supplier<String> bearerToken, HttpClient http) {
    this.baseUrl = stripTrailingSlash(baseUrl);
    this.bearerToken = bearerToken;
    this.http = http;
  }

  /** Convenience for tests / fixed tokens; production passes a {@link Supplier} reading the config. */
  public UnityCatalogClient(String baseUrl, String bearerToken) {
    this(baseUrl, () -> bearerToken);
  }

  /** Resolve {@code catalog.schema.table} to its UC table id and cloud storage location. */
  public TableInfo getTable(String fullName) throws IOException, InterruptedException {
    String url =
        baseUrl
            + "/api/2.1/unity-catalog/tables/"
            + URLEncoder.encode(fullName, StandardCharsets.UTF_8);
    HttpRequest req =
        baseRequest(url).header("Accept", "application/json").GET().build();
    JsonNode body = send(req);
    return new TableInfo(
        body.path("table_id").asText(null),
        body.path("storage_location").asText(null),
        body);
  }

  /** Vend temporary table credentials. {@code operation} is "READ" or "READ_WRITE". */
  public TemporaryCredentials getTemporaryCredentials(String tableId, String operation)
      throws IOException, InterruptedException {
    String url = baseUrl + "/api/2.1/unity-catalog/temporary-table-credentials";
    String payload =
        MAPPER
            .createObjectNode()
            .put("table_id", tableId)
            .put("operation", operation)
            .toString();
    HttpRequest req =
        baseRequest(url)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(payload, StandardCharsets.UTF_8))
            .build();
    JsonNode body = send(req);
    return new TemporaryCredentials(body);
  }

  private HttpRequest.Builder baseRequest(String url) {
    return HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(30))
        .header("Authorization", "Bearer " + bearerToken.get());
  }

  private JsonNode send(HttpRequest req) throws IOException, InterruptedException {
    HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
    if (resp.statusCode() / 100 != 2) {
      // Never include the response body: the temporary-table-credentials endpoint returns the vended
      // SAS in it. Status + redacted URI is enough to diagnose without leaking secrets.
      throw new IOException(
          "Unity Catalog API " + resp.statusCode() + " for " + Redact.text(req.uri().toString()));
    }
    return MAPPER.readTree(resp.body());
  }

  private static String stripTrailingSlash(String s) {
    return s != null && s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
  }

  /** Subset of the GET table response. */
  public static final class TableInfo {
    public final String tableId;
    public final String storageLocation;
    public final JsonNode raw;

    public TableInfo(String tableId, String storageLocation, JsonNode raw) {
      this.tableId = tableId;
      this.storageLocation = storageLocation;
      this.raw = raw;
    }
  }

  /**
   * Vended credentials. Cloud-specific blocks are exposed lazily; for Azure ADLS the relevant field
   * is {@code azure_user_delegation_sas.sas_token}.
   */
  public static final class TemporaryCredentials {
    public final JsonNode raw;

    public TemporaryCredentials(JsonNode raw) {
      this.raw = raw;
    }

    public Optional<String> azureSasToken() {
      JsonNode t = raw.path("azure_user_delegation_sas").path("sas_token");
      return t.isMissingNode() || t.isNull() ? Optional.empty() : Optional.of(t.asText());
    }

    public Optional<String> storageUrl() {
      JsonNode u = raw.path("url");
      return u.isMissingNode() || u.isNull() ? Optional.empty() : Optional.of(u.asText());
    }

    public Optional<Long> expirationTimeMillis() {
      JsonNode e = raw.path("expiration_time");
      return e.isMissingNode() || e.isNull() ? Optional.empty() : Optional.of(e.asLong());
    }
  }
}
