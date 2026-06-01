package io.dakotaoss.delta.auth;

import io.dakotaoss.delta.DeltaSinkConfig;

/** Builds the {@link CredentialProvider} for the configured {@code databricks.auth.type}. */
public final class Credentials {

  private Credentials() {}

  public static CredentialProvider fromConfig(DeltaSinkConfig c) {
    switch (c.authType()) {
      case DeltaSinkConfig.AUTH_PAT:
        // read per request so a rotated config value is picked up; nothing to refresh.
        return new StaticCredential(() -> c.token().value());
      case DeltaSinkConfig.AUTH_OAUTH_M2M:
        return new RefreshingCredential(
            new DatabricksOAuthMinter(c.workspaceUrl(), c.clientId(), () -> c.clientSecret().value()));
      case DeltaSinkConfig.AUTH_AZURE_ENTRA:
        return new RefreshingCredential(
            new AzureEntraMinter(c.azureTenantId(), c.clientId(), () -> c.clientSecret().value()));
      default:
        // unreachable: DeltaSinkConfig validates auth.type against the known set.
        throw new IllegalStateException("unknown " + DeltaSinkConfig.AUTH_TYPE + ": " + c.authType());
    }
  }
}
