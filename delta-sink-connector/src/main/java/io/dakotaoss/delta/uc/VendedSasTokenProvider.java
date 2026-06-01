package io.dakotaoss.delta.uc;

import io.dakotaoss.delta.util.Redact;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;

/**
 * ABFS {@link SASTokenProvider} that hands back each table's vended, directory-scoped SAS from
 * {@link VendedSasStore} at the request boundary. Wired per host via {@code
 * fs.azure.sas.token.provider.type.<host>} (+ {@code account.auth.type=SAS}).
 *
 * <p>This replaces the {@code fs.azure.sas.fixed.token.<host>} config key: the SAS stays in the
 * store (out of the Hadoop {@code Configuration}), and because the provider disambiguates by
 * request path, one cached FileSystem per storage host can serve many tables — so the JVM-global
 * FS-cache disable is no longer needed.
 *
 * <p>ABFS instantiates this by reflection, hence the public no-arg constructor (naming Hadoop's own
 * {@code services.FixedSASTokenProvider} for the provider type fails for lack of one).
 */
public final class VendedSasTokenProvider implements SASTokenProvider {

  private String accountName;

  public VendedSasTokenProvider() {}

  @Override
  public void initialize(Configuration configuration, String accountName) {
    // fallback host for getSASToken if ABFS passes a null/empty account
    this.accountName = accountName;
  }

  @Override
  public String getSASToken(String account, String fileSystem, String path, String operation)
      throws IOException {
    String host = (account == null || account.isEmpty()) ? accountName : account;
    String sas = VendedSasStore.instance().sasFor(host, fileSystem, path);
    if (sas == null) {
      // Redact masks the whole abfss:// URL, so this discloses neither the SAS nor the layout.
      throw new IOException(
          "No vended SAS registered for "
              + Redact.text("abfss://" + fileSystem + "@" + host + "/" + path));
    }
    return sas;
  }
}
