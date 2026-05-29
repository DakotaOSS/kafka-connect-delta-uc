package io.dakotaoss.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dakotaoss.delta.uc.UnityCatalogClient;
import org.junit.jupiter.api.Test;

class CredentialsParsingTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void parsesAzureSasUrlAndExpiry() throws Exception {
    String json =
        "{\"azure_user_delegation_sas\":{\"sas_token\":\"sig=q\"},"
            + "\"url\":\"abfss://c@a.dfs.core.windows.net/t\",\"expiration_time\":123}";
    UnityCatalogClient.TemporaryCredentials c =
        new UnityCatalogClient.TemporaryCredentials(mapper.readTree(json));
    assertEquals("sig=q", c.azureSasToken().orElseThrow());
    assertEquals("abfss://c@a.dfs.core.windows.net/t", c.storageUrl().orElseThrow());
    assertEquals(123L, c.expirationTimeMillis().orElseThrow());
  }

  @Test
  void absentFieldsYieldEmptyOptionals() throws Exception {
    UnityCatalogClient.TemporaryCredentials c =
        new UnityCatalogClient.TemporaryCredentials(mapper.readTree("{}"));
    assertFalse(c.azureSasToken().isPresent());
    assertFalse(c.storageUrl().isPresent());
    assertFalse(c.expirationTimeMillis().isPresent());
    assertTrue(true);
  }
}
