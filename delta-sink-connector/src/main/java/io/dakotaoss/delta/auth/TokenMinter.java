package io.dakotaoss.delta.auth;

/** Mints a fresh bearer token from an identity provider (OAuth2 client-credentials). */
public interface TokenMinter {

  Minted mint() throws Exception;

  /** A minted token and its lifetime in seconds (the IdP's {@code expires_in}). */
  final class Minted {
    public final String token;
    public final long ttlSeconds;

    public Minted(String token, long ttlSeconds) {
      this.token = token;
      this.ttlSeconds = ttlSeconds;
    }
  }
}
