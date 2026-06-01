package io.dakotaoss.delta.uc;

import io.unitycatalog.client.auth.TokenProvider;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

/**
 * {@link TokenProvider} for the catalog-commit path. Hands back a bearer token (Databricks PAT or
 * Entra/AAD access token for the Azure Databricks resource); the UC REST client sets it as {@code
 * Authorization: Bearer}.
 *
 * <p>The token is sourced from a {@link Supplier} read on each call, not captured as a long-lived
 * field here, so a re-minted token (refresh near expiry) is picked up without rebuilding the
 * provider, and we hold no extra durable copy of the secret.
 */
public final class BearerTokenProvider implements TokenProvider {

  private final Supplier<String> token;

  public BearerTokenProvider(Supplier<String> token) {
    this.token = token;
  }

  @Override
  public void initialize(Map<String, String> configs) {
    // no-op: the token is supplied directly.
  }

  @Override
  public String accessToken() {
    return token.get();
  }

  @Override
  public Map<String, String> configs() {
    return Collections.emptyMap();
  }
}
