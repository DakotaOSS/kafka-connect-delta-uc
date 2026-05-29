package io.dakotaoss.delta.uc;

import io.dakotaoss.delta.model.TableTarget;
import org.apache.kafka.connect.errors.ConnectException;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Production {@link TableResolver}: maps a topic to a {@code catalog.schema.table} name, asks Unity
 * Catalog for the storage location + table id, vends short-lived READ_WRITE credentials, and packages
 * the ABFS configuration the Kernel engine needs to write into the managed table's storage.
 *
 * <p>The vended Azure SAS is installed as a Hadoop ABFS "fixed SAS token" for the storage account
 * host, which is how the default Kernel engine authenticates to {@code abfss://} without a long-lived
 * service-principal secret.
 */
public final class UcTableResolver implements TableResolver {

  private final UnityCatalogClient uc;
  private final String tableNameFormat; // e.g. "main.ingestion.${topic}"
  private final List<String> partitionColumns;

  public UcTableResolver(UnityCatalogClient uc, String tableNameFormat, List<String> partitionColumns) {
    this.uc = uc;
    this.tableNameFormat = tableNameFormat;
    this.partitionColumns = partitionColumns == null ? Collections.emptyList() : partitionColumns;
  }

  @Override
  public TableTarget resolve(String topic) {
    String fullName = tableNameFormat.replace("${topic}", sanitize(topic));
    try {
      UnityCatalogClient.TableInfo table = uc.getTable(fullName);
      if (table.tableId == null || table.storageLocation == null) {
        throw new ConnectException(
            "Unity Catalog returned no table_id/storage_location for " + fullName);
      }
      UnityCatalogClient.TemporaryCredentials creds =
          uc.getTemporaryCredentials(table.tableId, "READ_WRITE");

      Map<String, String> hadoop = abfsConfig(table.storageLocation, creds);
      return new TableTarget(
          fullName, table.storageLocation, table.tableId, partitionColumns, hadoop);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConnectException("Interrupted resolving UC table " + fullName, e);
    } catch (Exception e) {
      throw new ConnectException("Failed to resolve UC table " + fullName, e);
    }
  }

  /** Build ABFS fixed-SAS Hadoop config keyed on the storage-account host. */
  public static Map<String, String> abfsConfig(
      String storageLocation, UnityCatalogClient.TemporaryCredentials creds) {
    Map<String, String> conf = new HashMap<>();
    String sas = creds.azureSasToken().orElse(null);
    if (sas == null) {
      // Non-Azure cloud, or a different credential shape - leave engine to its default chain.
      return conf;
    }
    String host = URI.create(storageLocation).getHost(); // <account>.dfs.core.windows.net
    if (host == null) {
      return conf;
    }
    // Lower-case: SAS-scoped account keys are host-suffixed; a differently-cased host from UC would
    // not match the abfss:// host ABFS resolves, silently dropping the SAS and 403ing.
    host = host.toLowerCase(java.util.Locale.ROOT);
    // Fixed-SAS auth: account auth type SAS, vended token as the fixed token. Deliberately do NOT
    // set fs.azure.sas.token.provider.type: with a fixed token present and no provider type named,
    // the ABFS driver constructs services.FixedSASTokenProvider(token) itself. Naming that class as
    // the provider type fails at runtime - it has no no-arg constructor and Hadoop's ReflectionUtils
    // requires one.
    conf.put("fs.azure.account.auth.type." + host, "SAS");
    conf.put("fs.azure.sas.fixed.token." + host, sas);
    // The vended SAS is scoped to the table's directory. ABFS otherwise probes HNS support by
    // calling getAccessControl on the *container root*, which is outside the SAS scope and 403s.
    // ADLS Gen2 storage is always HNS-enabled, so declare it and skip the probe. Host-suffixed only:
    // an un-suffixed global key would force HNS on co-located connectors sharing this JVM.
    conf.put("fs.azure.account.hns.enabled." + host, "true");
    // UC vends a SAS scoped to each table's own directory. Hadoop caches one FileSystem per
    // storage-account host, so without this a second table on the same account would reuse the
    // first table's (out-of-scope) SAS and get 403s. Disable the cache so each table uses its own.
    conf.put("fs.abfss.impl.disable.cache", "true");
    return conf;
  }

  private static String sanitize(String topic) {
    // Kafka topics allow chars not valid in table names; normalise conservatively.
    return topic.replaceAll("[^A-Za-z0-9_]", "_");
  }
}
