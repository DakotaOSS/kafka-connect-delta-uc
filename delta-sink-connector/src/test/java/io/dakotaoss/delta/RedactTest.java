package io.dakotaoss.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.dakotaoss.delta.util.Redact;
import org.junit.jupiter.api.Test;

class RedactTest {

  @Test
  void nullIsPassedThrough() {
    assertNull(Redact.text(null));
    assertNull(Redact.message(null));
  }

  @Test
  void masksSasQueryOnAbfssUrl() {
    String in = "Operation failed on abfss://cont@acct.dfs.core.windows.net/t?sig=SECRET&se=2026";
    String out = Redact.text(in);
    assertFalse(out.contains("sig=SECRET"), "SAS signature must be gone");
    assertFalse(out.contains("se=2026"), "SAS params must be gone");
  }

  @Test
  void masksStorageAccountAndContainerLayout() {
    // the abfss:// path discloses the storage account / container layout even with the SAS stripped.
    // Both the no-query path form and the query form must hide account + container.
    String pathOnly = "wrote file abfss://cont@acct.dfs.core.windows.net/__unitystorage/x/y.parquet";
    String withQuery = "GET abfss://cont@acct.dfs.core.windows.net/t/_delta_log/00.json?sig=ZZ";
    for (String in : new String[] {pathOnly, withQuery}) {
      String out = Redact.text(in);
      assertFalse(out.contains("acct.dfs.core.windows.net"), "storage account host must be masked: " + out);
      assertFalse(out.contains("cont@"), "container must be masked: " + out);
      assertFalse(out.contains("__unitystorage"), "storage path layout must be masked: " + out);
    }
  }

  @Test
  void masksBearerTokenAndSasTokenJson() {
    assertFalse(Redact.text("Authorization: Bearer dapiABC.def-123").contains("dapiABC"));
    assertFalse(Redact.text("{\"sas_token\":\"sv=2024&sig=qqq\"}").contains("sv=2024"));
  }

  @Test
  void leavesNonSecretTextIntact() {
    String in = "Committed app:orders-0 to main.ingestion.orders at version 7";
    assertEquals(in, Redact.text(in));
  }

  @Test
  void messageRedactsThrowableToString() {
    Exception e = new RuntimeException("403 on abfss://cont@acct.dfs.core.windows.net/t?sig=SECRET");
    String out = Redact.message(e);
    assertFalse(out.contains("sig=SECRET"));
    assertFalse(out.contains("acct.dfs.core.windows.net"));
    assertTrue(out.contains("RuntimeException"), "throwable type should remain for diagnosis");
  }

  @Test
  void messageRecursesCauseChainAndRedacts() {
    // a SAS buried in a nested cause must not survive: message() recurses the whole chain, redacting
    // each level (the top-level toString() alone would omit the cause, but a raw rethrow could leak it).
    Throwable leaf =
        new IllegalStateException("ABFS GET abfss://cont@acct.dfs.core.windows.net/t?sig=DEEPSECRET");
    Throwable mid = new java.io.IOException("write failed", leaf);
    Throwable top = new RuntimeException("commit failed", mid);
    String out = Redact.message(top);
    assertFalse(out.contains("sig=DEEPSECRET"), "nested SAS must be redacted");
    assertFalse(out.contains("acct.dfs.core.windows.net"), "nested storage host must be redacted");
    assertTrue(out.contains("RuntimeException") && out.contains("IOException") && out.contains("IllegalStateException"),
        "all cause levels should appear (redacted) for diagnosis");
  }
}
