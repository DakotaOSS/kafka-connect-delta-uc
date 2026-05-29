package io.dakotaoss.delta;

import io.dakotaoss.delta.util.Redact;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Configuration surface for the DeltaTables sink connector. */
public final class DeltaSinkConfig extends AbstractConfig {

  public static final String WORKSPACE_URL = "databricks.workspace.url";
  public static final String TOKEN = "databricks.token";
  public static final String TABLE_NAME_FORMAT = "table.name.format";
  public static final String PARTITION_COLUMNS = "partition.columns";
  public static final String FLUSH_SIZE = "flush.size";
  public static final String FLUSH_BYTES = "flush.bytes";
  public static final String FLUSH_INTERVAL_MS = "flush.interval.ms";

  // UC REST + credential vending assume TLS; reject http:// and scheme-less values at config time
  // rather than failing later on the first request. Redact the echoed value in case a token rode in.
  private static final ConfigDef.Validator HTTPS_URL =
      new ConfigDef.Validator() {
        @Override
        public void ensureValid(String name, Object value) {
          String s = (String) value;
          if (s == null || s.trim().isEmpty()) {
            throw new ConfigException(name, s, "must be set to an https:// workspace URL");
          }
          final URI uri;
          try {
            uri = new URI(s.trim());
          } catch (URISyntaxException e) {
            throw new ConfigException(name, Redact.text(s), "is not a valid URL: " + e.getReason());
          }
          if (!"https".equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null) {
            throw new ConfigException(
                name, Redact.text(s), "must be an https:// URL with a host (http:// is not allowed)");
          }
        }

        @Override
        public String toString() {
          return "https:// URL";
        }
      };

  public static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              WORKSPACE_URL,
              ConfigDef.Type.STRING,
              ConfigDef.NO_DEFAULT_VALUE,
              HTTPS_URL,
              ConfigDef.Importance.HIGH,
              "Databricks workspace base URL, e.g. https://adb-1234567890.1.azuredatabricks.net")
          .define(
              TOKEN,
              ConfigDef.Type.PASSWORD,
              ConfigDef.Importance.HIGH,
              "OAuth/PAT bearer token for the Unity Catalog REST API. Principal must hold "
                  + "EXTERNAL USE SCHEMA on the target schema.")
          .define(
              TABLE_NAME_FORMAT,
              ConfigDef.Type.STRING,
              "main.ingestion.${topic}",
              ConfigDef.Importance.HIGH,
              "Three-part UC name; ${topic} is substituted with the (sanitised) topic name.")
          .define(
              PARTITION_COLUMNS,
              ConfigDef.Type.LIST,
              Collections.emptyList(),
              ConfigDef.Importance.MEDIUM,
              "Partition columns used only when this connector creates a new table.")
          .define(
              FLUSH_SIZE,
              ConfigDef.Type.INT,
              500,
              ConfigDef.Importance.MEDIUM,
              "Buffer this many records per topic-partition before committing a Delta transaction. "
                  + "0 disables the row-count dial.")
          .define(
              FLUSH_BYTES,
              ConfigDef.Type.LONG,
              0L,
              ConfigDef.Importance.MEDIUM,
              "Flush when a partition's buffered bytes (approx) reach this, for target file size "
                  + "(e.g. 134217728 = 128 MiB). 0 disables the byte-size dial.")
          .define(
              FLUSH_INTERVAL_MS,
              ConfigDef.Type.LONG,
              5_000L,
              ConfigDef.Importance.MEDIUM,
              "Max time (ms) to buffer a partition before committing, even below flush.size. "
                  + "Drives micro-batch latency; 5s mirrors Zerobus.");

  public DeltaSinkConfig(Map<String, String> props) {
    super(CONFIG_DEF, props);
    // All three dials off means nothing ever triggers a commit -> the buffer grows without bound
    // and the task OOMs. Cross-field, so it can't live in a single-key Validator; fail fast here.
    if (flushSize() <= 0 && flushBytes() <= 0 && flushIntervalMs() <= 0) {
      throw new ConfigException(
          "At least one flush dial must be enabled (> 0): "
              + FLUSH_SIZE + ", " + FLUSH_BYTES + ", or " + FLUSH_INTERVAL_MS
              + "; all disabled would buffer unbounded.");
    }
  }

  public String workspaceUrl() {
    return getString(WORKSPACE_URL);
  }

  public Password token() {
    return getPassword(TOKEN);
  }

  public String tableNameFormat() {
    return getString(TABLE_NAME_FORMAT);
  }

  public List<String> partitionColumns() {
    return getList(PARTITION_COLUMNS);
  }

  public int flushSize() {
    return getInt(FLUSH_SIZE);
  }

  public long flushBytes() {
    return getLong(FLUSH_BYTES);
  }

  public long flushIntervalMs() {
    return getLong(FLUSH_INTERVAL_MS);
  }
}
