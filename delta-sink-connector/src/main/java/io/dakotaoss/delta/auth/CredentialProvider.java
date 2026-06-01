package io.dakotaoss.delta.auth;

import java.util.function.Supplier;

/**
 * A bearer-token source that stays valid on its own: {@link #get()} returns a currently-valid
 * token, minting/refreshing internally as needed so callers never handle expiry. Extends {@link
 * Supplier} so it drops straight into the UC clients' existing token seam ({@code
 * UnityCatalogClient}, {@code UnityCatalogCommitter}).
 */
public interface CredentialProvider extends Supplier<String> {

  /**
   * Drop any cached token so the next {@link #get()} re-mints. Called after an auth rejection
   * (401).
   */
  default void invalidate() {}
}
