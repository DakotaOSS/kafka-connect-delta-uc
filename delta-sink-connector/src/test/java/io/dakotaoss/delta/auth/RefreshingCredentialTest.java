package io.dakotaoss.delta.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class RefreshingCredentialTest {

  /** Hands out t1, t2, ... on each successful mint; can be flipped to fail. */
  private static final class CountingMinter implements TokenMinter {
    final AtomicInteger calls = new AtomicInteger();
    final long ttlSeconds;
    volatile boolean fail = false;

    CountingMinter(long ttlSeconds) {
      this.ttlSeconds = ttlSeconds;
    }

    @Override
    public Minted mint() throws Exception {
      if (fail) {
        throw new java.io.IOException("idp down");
      }
      return new Minted("t" + calls.incrementAndGet(), ttlSeconds);
    }
  }

  // minCushion=0, fraction=0.8 so refresh fires deterministically at exactly 0.8*ttl.
  private static RefreshingCredential cred(CountingMinter m, AtomicLong nowMs) {
    return new RefreshingCredential(m, nowMs::get, 0.8, 0);
  }

  @Test
  void cachesWithinLifetime() {
    AtomicLong now = new AtomicLong(0);
    CountingMinter m = new CountingMinter(100); // ttl 100s
    RefreshingCredential c = cred(m, now);
    assertEquals("t1", c.get());
    now.set(50_000); // 50s in, before the 80s refresh point
    assertEquals("t1", c.get());
    assertEquals(1, m.calls.get(), "must not re-mint while fresh");
  }

  @Test
  void refreshesProactivelyBeforeExpiry() {
    AtomicLong now = new AtomicLong(0);
    CountingMinter m = new CountingMinter(100);
    RefreshingCredential c = cred(m, now);
    assertEquals("t1", c.get());
    now.set(70_000);
    assertEquals("t1", c.get(), "still cached before the ~80% refresh point");
    now.set(85_000);
    assertEquals("t2", c.get(), "refreshed past ~80% of lifetime, well before the 100s expiry");
    assertEquals(2, m.calls.get());
  }

  @Test
  void invalidateForcesRefresh() {
    AtomicLong now = new AtomicLong(0);
    CountingMinter m = new CountingMinter(100);
    RefreshingCredential c = cred(m, now);
    assertEquals("t1", c.get());
    c.invalidate();
    assertEquals("t2", c.get(), "invalidate() drops the cache so next get() re-mints");
  }

  @Test
  void toleratesMintFailureWhileCachedTokenValid() {
    AtomicLong now = new AtomicLong(0);
    CountingMinter m = new CountingMinter(100);
    RefreshingCredential c = cred(m, now);
    assertEquals("t1", c.get());
    now.set(85_000); // past refresh (80s), before expiry (100s)
    m.fail = true;
    assertEquals("t1", c.get(), "serve the still-valid cached token through a transient mint failure");
  }

  @Test
  void throwsWhenExpiredAndMintFails() {
    AtomicLong now = new AtomicLong(0);
    CountingMinter m = new CountingMinter(100);
    RefreshingCredential c = cred(m, now);
    assertEquals("t1", c.get());
    now.set(101_000); // past hard expiry
    m.fail = true;
    assertThrows(IllegalStateException.class, c::get);
  }

  @Test
  void refreshIsSingleFlightUnderConcurrency() throws Exception {
    AtomicLong now = new AtomicLong(0);
    CountingMinter m = new CountingMinter(100);
    RefreshingCredential c = cred(m, now);
    c.get(); // t1, one mint
    now.set(90_000); // past refresh point: the next get() must refresh
    int n = 8;
    Thread[] ts = new Thread[n];
    for (int i = 0; i < n; i++) {
      ts[i] = new Thread(c::get);
    }
    for (Thread t : ts) {
      t.start();
    }
    for (Thread t : ts) {
      t.join();
    }
    assertEquals(2, m.calls.get(), "concurrent callers at the refresh point share one mint");
  }
}
