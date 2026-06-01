package io.dakotaoss.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.dakotaoss.delta.uc.BearerTokenProvider;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class BearerTokenProviderTest {

  @Test
  void accessTokenIsReadFromSupplierEachCall() {
    // the provider holds a Supplier, not a captured String, so a refreshed token is reflected
    // without
    // reconstructing the provider (the refresh hook the SPEC calls out).
    AtomicReference<String> token = new AtomicReference<>("t1");
    BearerTokenProvider provider = new BearerTokenProvider(token::get);
    assertEquals("t1", provider.accessToken());
    token.set("t2");
    assertEquals("t2", provider.accessToken());
  }
}
