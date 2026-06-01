package io.dakotaoss.delta.auth;

import java.util.function.Supplier;

/**
 * A fixed, externally-managed token (PAT or long-lived service-principal token). Read from the
 * supplied source on each call so a rotated config value is picked up and we keep no extra durable
 * copy of the secret. Durability here is the token's own lifetime — there is nothing to refresh.
 */
public final class StaticCredential implements CredentialProvider {

  private final Supplier<String> source;

  public StaticCredential(Supplier<String> source) {
    this.source = source;
  }

  @Override
  public String get() {
    return source.get();
  }
}
