package io.dakotaoss.delta.uc;

import io.dakotaoss.delta.model.TableTarget;
import io.dakotaoss.delta.util.Redact;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.connect.errors.ConnectException;

/**
 * Production {@link TableResolver}: maps a topic to a {@code catalog.schema.table} name, asks Unity
 * Catalog for the storage location + table id, vends short-lived READ_WRITE credentials, and
 * packages the ABFS configuration the Kernel engine needs to write into the managed table's
 * storage.
 *
 * <p>The vended Azure SAS is held as {@code char[]} in {@link VendedSasStore} (never in the Hadoop
 * {@code Configuration}) and handed to ABFS at the request boundary by a per-host {@link
 * VendedSasTokenProvider}; the returned config only wires that provider for the storage account
 * host. This is how the default Kernel engine authenticates to {@code abfss://} without a
 * long-lived service-principal secret, while one cached FileSystem per host still serves many
 * tables.
 */
public final class UcTableResolver implements TableResolver {

  // ${topic} (whole topic) or ${topic[N]} (Nth dot-segment, 0-indexed).
  private static final Pattern TOPIC_TOKEN = Pattern.compile("\\$\\{topic(?:\\[(\\d+)\\])?\\}");

  // A topic value substituted into a UC name must already be a valid identifier part. UC
  // identifiers
  // cap at 255 chars.
  private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9_]+");
  private static final int MAX_IDENTIFIER_LEN = 255;

  private final UnityCatalogClient uc;
  private final String
      tableNameFormat; // e.g. "main.ingestion.${topic}" or "bronze.${topic[0]}.${topic[2]}"
  private final Map<String, String>
      topicToTable; // explicit topic -> catalog.schema.table overrides
  private final List<String> partitionColumns;

  /** Convenience constructor with no explicit per-topic overrides. */
  public UcTableResolver(
      UnityCatalogClient uc, String tableNameFormat, List<String> partitionColumns) {
    this(uc, tableNameFormat, Collections.emptyMap(), partitionColumns);
  }

  public UcTableResolver(
      UnityCatalogClient uc,
      String tableNameFormat,
      Map<String, String> topicToTable,
      List<String> partitionColumns) {
    this.uc = uc;
    this.tableNameFormat = tableNameFormat;
    this.topicToTable = topicToTable == null ? Collections.emptyMap() : topicToTable;
    this.partitionColumns = partitionColumns == null ? Collections.emptyList() : partitionColumns;
  }

  @Override
  public TableTarget resolve(String topic) {
    String fullName = resolveName(tableNameFormat, topicToTable, topic);
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
      // Drop the raw cause: today resolve()'s causes are pre-redacted by the UC client, but a
      // future
      // refactor could surface a raw ABFS exception whose message embeds a vended SAS, and the
      // chained
      // cause would reach Connect's task-failure log verbatim. Keep only the redacted message.
      throw new ConnectException(
          "Failed to resolve UC table " + fullName + ": " + Redact.message(e));
    }
  }

  /**
   * The full {@code catalog.schema.table} a topic routes to, without any UC call (for auto-create).
   */
  public String nameFor(String topic) {
    return resolveName(tableNameFormat, topicToTable, topic);
  }

  /**
   * Register the vended SAS in {@link VendedSasStore} and return the ABFS Hadoop config (keyed on
   * the storage-account host) that points ABFS at {@link VendedSasTokenProvider}. The SAS itself is
   * kept in the store (as {@code char[]}), never in the returned config.
   */
  public static Map<String, String> abfsConfig(
      String storageLocation, UnityCatalogClient.TemporaryCredentials creds) {
    Map<String, String> conf = new HashMap<>();
    String sas = creds.azureSasToken().orElse(null);
    if (sas == null) {
      // Non-Azure cloud, or a different credential shape - leave engine to its default chain.
      return conf;
    }
    URI uri = URI.create(storageLocation);
    String host = uri.getHost(); // <account>.dfs.core.windows.net
    if (host == null) {
      return conf;
    }
    // Lower-case: SAS-scoped account keys are host-suffixed; a differently-cased host from UC would
    // not match the abfss:// host ABFS resolves, silently dropping the SAS and 403ing.
    host = host.toLowerCase(java.util.Locale.ROOT);
    // Hold the SAS in the store, scoped to this table's container + directory, instead of putting
    // it
    // in the Configuration. VendedSasTokenProvider returns it per request path, so the SAS never
    // lands in the config and one cached FileSystem per host can serve many tables.
    VendedSasStore.instance().put(host, uri.getUserInfo(), uri.getPath(), sas.toCharArray());
    // Provider-based SAS auth: account auth type SAS + our provider (it has the required no-arg
    // ctor).
    conf.put("fs.azure.account.auth.type." + host, "SAS");
    conf.put("fs.azure.sas.token.provider.type." + host, VendedSasTokenProvider.class.getName());
    // The vended SAS is scoped to the table's directory. ABFS otherwise probes HNS support by
    // calling getAccessControl on the *container root*, which is outside the SAS scope and 403s.
    // ADLS Gen2 storage is always HNS-enabled, so declare it and skip the probe. Host-suffixed
    // only:
    // an un-suffixed global key would force HNS on co-located connectors sharing this JVM.
    conf.put("fs.azure.account.hns.enabled." + host, "true");
    return conf;
  }

  /**
   * Resolve a topic to its full {@code catalog.schema.table}: an explicit {@code topic.to.table}
   * override wins; otherwise render {@code table.name.format}. Validates the result is three parts.
   */
  public static String resolveName(String format, Map<String, String> overrides, String topic) {
    String mapped = overrides.get(topic);
    String full = mapped != null ? mapped : render(format, topic);
    String[] parts = full.split("\\.", -1);
    if (parts.length != 3 || parts[0].isEmpty() || parts[1].isEmpty() || parts[2].isEmpty()) {
      throw new ConnectException(
          "Routing for topic '"
              + topic
              + "' resolved to '"
              + full
              + "', which is not a catalog.schema.table name");
    }
    return full;
  }

  // Substitute ${topic} (whole topic) and ${topic[N]} (Nth dot-segment, 0-indexed); every
  // substituted value must already be a valid identifier part (see identifierPart).
  private static String render(String format, String topic) {
    String[] seg = topic.split("\\.", -1);
    Matcher m = TOPIC_TOKEN.matcher(format);
    StringBuilder out = new StringBuilder();
    while (m.find()) {
      String idx = m.group(1);
      String rep;
      if (idx == null) {
        rep = identifierPart(topic, topic);
      } else {
        final int i;
        try {
          i = Integer.parseInt(idx);
        } catch (NumberFormatException nfe) {
          // idx is digits but may overflow int; surface a clear config error, not a raw NFE.
          throw new ConnectException(
              "table.name.format segment index ${topic[" + idx + "]} is out of range");
        }
        if (i >= seg.length) {
          throw new ConnectException(
              "table.name.format references ${topic["
                  + i
                  + "]} but topic '"
                  + topic
                  + "' has only "
                  + seg.length
                  + " dot-segment(s)");
        }
        rep = identifierPart(seg[i], topic);
      }
      m.appendReplacement(out, Matcher.quoteReplacement(rep));
    }
    m.appendTail(out);
    return out.toString();
  }

  // Validate a topic-derived value as a UC identifier part. We deliberately reject out-of-set
  // characters rather than fold them to '_': folding is non-injective (orders.eu, orders/eu,
  // orders-eu would all collapse to orders_eu), so under an untrusted/regex subscription a crafted
  // topic could collide onto a victim's table. Dotted topics route via ${topic[N]} segment tokens
  // (the dot is the delimiter); anything else routes via an explicit topic.to.table mapping, which
  // is
  // matched on the exact topic and never transformed.
  private static String identifierPart(String value, String topic) {
    if (value.length() > MAX_IDENTIFIER_LEN) {
      throw new ConnectException(
          "Routing for topic '"
              + topic
              + "' produced an identifier part of "
              + value.length()
              + " chars, over the "
              + MAX_IDENTIFIER_LEN
              + " limit");
    }
    if (!IDENTIFIER.matcher(value).matches()) {
      throw new ConnectException(
          "Routing for topic '"
              + topic
              + "' produced '"
              + value
              + "', which is not a valid UC identifier part [A-Za-z0-9_]; use ${topic[N]} segment "
              + "tokens or an explicit topic.to.table mapping");
    }
    return value;
  }
}
