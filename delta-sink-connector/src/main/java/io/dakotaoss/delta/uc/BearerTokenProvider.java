package io.dakotaoss.delta.uc;

import io.unitycatalog.client.auth.TokenProvider;
import java.util.Collections;
import java.util.Map;

/**
 * {@link TokenProvider} for the catalog-commit path. Hands back a bearer token (Databricks PAT or
 * Entra/AAD access token for the Azure Databricks resource); the UC REST client sets it as
 * {@code Authorization: Bearer}.
 *
 * <p>Token is a fixed string captured at construction. Tokens expire, so long-running Connect tasks
 * want a {@link java.util.function.Supplier} that re-mints near expiry instead.
 */
public final class BearerTokenProvider implements TokenProvider {

  private final String token;

  public BearerTokenProvider(String token) {
    this.token = token;
  }

  @Override
  public void initialize(Map<String, String> configs) {
    // no-op: the token is supplied directly.
  }

  @Override
  public String accessToken() {
    return token;
  }

  @Override
  public Map<String, String> configs() {
    return Collections.emptyMap();
  }
}
