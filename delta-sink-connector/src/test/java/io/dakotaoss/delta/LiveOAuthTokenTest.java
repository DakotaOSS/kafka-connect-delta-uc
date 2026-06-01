package io.dakotaoss.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.dakotaoss.delta.auth.AzureEntraMinter;
import io.dakotaoss.delta.auth.CredentialProvider;
import io.dakotaoss.delta.auth.DatabricksOAuthMinter;
import io.dakotaoss.delta.auth.RefreshingCredential;
import io.dakotaoss.delta.auth.TokenMinter;
import io.dakotaoss.delta.uc.UnityCatalogClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Live: mint a token from a service principal via OAuth client-credentials and prove Unity Catalog
 * accepts it. Skipped unless {@code DATABRICKS_CLIENT_ID} is set, so the offline build never runs it.
 *
 * <p>If {@code AZURE_TENANT_ID} is set the test exercises {@code azure-entra}; otherwise Databricks
 * {@code oauth-m2m}. Required env: {@code DATABRICKS_HOST}, {@code DATABRICKS_CLIENT_ID},
 * {@code DATABRICKS_CLIENT_SECRET}, optional {@code AZURE_TENANT_ID}, {@code UC_TABLE} (a table the SP
 * can read; the SP must hold {@code EXTERNAL USE SCHEMA} on its schema).
 */
class LiveOAuthTokenTest {

  @Test
  @EnabledIfEnvironmentVariable(named = "DATABRICKS_CLIENT_ID", matches = ".+")
  void mintsServicePrincipalTokenAndAuthenticatesToUc() throws Exception {
    String host = System.getenv("DATABRICKS_HOST");
    String clientId = System.getenv("DATABRICKS_CLIENT_ID");
    String secret = System.getenv("DATABRICKS_CLIENT_SECRET");
    String tenant = System.getenv("AZURE_TENANT_ID");
    String table = System.getenv().getOrDefault("UC_TABLE", "main.ingestion.delta_sink_smoke");

    TokenMinter minter =
        (tenant != null && !tenant.isEmpty())
            ? new AzureEntraMinter(tenant, clientId, () -> secret)
            : new DatabricksOAuthMinter(host, clientId, () -> secret);
    CredentialProvider cred = new RefreshingCredential(minter);

    String token = cred.get();
    assertNotNull(token);
    assertFalse(token.isEmpty(), "provider must mint a non-empty token");
    assertEquals(token, cred.get(), "second get() returns the cached token (refresh not yet due)");

    // the minted token must authenticate against live UC
    UnityCatalogClient uc = new UnityCatalogClient(host, cred);
    UnityCatalogClient.TableInfo info = uc.getTable(table);
    assertNotNull(info.tableId, "minted SP token must be accepted by the UC tables API");
    System.out.println("[LIVE] minted SP token authenticated; resolved " + table + " -> " + info.tableId);
  }
}
