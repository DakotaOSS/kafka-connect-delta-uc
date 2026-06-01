package io.dakotaoss.delta.auth;

import io.dakotaoss.delta.util.Redact;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps a {@link TokenMinter} with a cache that refreshes <b>proactively</b>, well before expiry,
 * so callers never see an expired token and never trip an auth-retry storm. Refresh is
 * single-flight (concurrent callers share one mint under the lock); a mint failure while the cached
 * token is still valid is tolerated — serve the cached token and try again next call — so a
 * transient IdP blip is invisible. Thread-safe.
 *
 * <p>Refresh fires once {@code REFRESH_FRACTION} of the token's stated lifetime has elapsed,
 * leaving a cushion (never below {@code MIN_CUSHION_MS}) for clock skew, mint latency, and retries.
 */
public final class RefreshingCredential implements CredentialProvider {

  private static final Logger LOG = LoggerFactory.getLogger(RefreshingCredential.class);
  private static final double REFRESH_FRACTION = 0.8;
  private static final long MIN_CUSHION_MS = 60_000;

  private final TokenMinter minter;
  private final LongSupplier clockMs;
  private final double refreshFraction;
  private final long minCushionMs;

  private final Object lock = new Object();
  private String token;
  private long expiresAtMs = Long.MIN_VALUE; // hard expiry: past this the cached token is unusable
  private long refreshAtMs = Long.MIN_VALUE; // proactive refresh point, strictly before expiresAtMs

  public RefreshingCredential(TokenMinter minter) {
    this(minter, System::currentTimeMillis, REFRESH_FRACTION, MIN_CUSHION_MS);
  }

  // visible for tests: inject a clock + cushion params so refresh timing is deterministic.
  RefreshingCredential(
      TokenMinter minter, LongSupplier clockMs, double refreshFraction, long minCushionMs) {
    this.minter = minter;
    this.clockMs = clockMs;
    this.refreshFraction = refreshFraction;
    this.minCushionMs = minCushionMs;
  }

  @Override
  public String get() {
    synchronized (lock) {
      long now = clockMs.getAsLong();
      if (token != null && now < refreshAtMs) {
        return token;
      }
      try {
        refresh(now);
      } catch (Exception e) {
        if (token != null && now < expiresAtMs) {
          LOG.warn(
              "token refresh failed; serving cached token (valid {}s more): {}",
              (expiresAtMs - now) / 1000,
              Redact.message(e));
          return token;
        }
        throw new IllegalStateException(
            "token mint failed and no valid cached token: " + Redact.message(e), e);
      }
      return token;
    }
  }

  @Override
  public void invalidate() {
    synchronized (lock) {
      refreshAtMs = Long.MIN_VALUE;
      expiresAtMs = Long.MIN_VALUE;
    }
  }

  private void refresh(long now) throws Exception {
    TokenMinter.Minted m = minter.mint();
    long ttlMs = Math.max(0, m.ttlSeconds * 1000L);
    long cushion = Math.max(minCushionMs, (long) (ttlMs * (1 - refreshFraction)));
    this.token = m.token;
    this.expiresAtMs = now + ttlMs;
    this.refreshAtMs = now + Math.max(0, ttlMs - cushion);
  }
}
