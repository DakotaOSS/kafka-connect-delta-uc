package io.dakotaoss.delta;

import io.dakotaoss.delta.util.Redact;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Configuration surface for the DeltaTables sink connector. */
public final class DeltaSinkConfig extends AbstractConfig {

  public static final String WORKSPACE_URL = "databricks.workspace.url";
  public static final String TOKEN = "databricks.token";
  public static final String AUTH_TYPE = "databricks.auth.type";
  public static final String CLIENT_ID = "databricks.client.id";
  public static final String CLIENT_SECRET = "databricks.client.secret";
  public static final String AZURE_TENANT_ID = "azure.tenant.id";
  // databricks.auth.type values
  public static final String AUTH_PAT = "pat";
  public static final String AUTH_OAUTH_M2M = "oauth-m2m";
  public static final String AUTH_AZURE_ENTRA = "azure-entra";
  public static final String AUTO_CREATE_TABLES = "auto.create.tables";
  public static final String WAREHOUSE_ID = "databricks.warehouse.id";
  public static final String SCHEMA_EVOLUTION = "schema.evolution";
  // schema.evolution values
  public static final String EVOLVE_NONE = "none";
  public static final String EVOLVE_ADD = "add";
  public static final String TABLE_NAME_FORMAT = "table.name.format";
  public static final String TOPIC_TO_TABLE = "topic.to.table";
  public static final String PARTITION_COLUMNS = "partition.columns";
  public static final String FLUSH_SIZE = "flush.size";
  public static final String FLUSH_BYTES = "flush.bytes";
  public static final String FLUSH_INTERVAL_MS = "flush.interval.ms";
  public static final String FLUSH_CONCURRENCY = "flush.concurrency";

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

  // Each entry maps one topic to a full UC name: "<topic>:<catalog>.<schema>.<table>". Kafka topic
  // names contain no ':' and UC identifier parts no '.', so the shapes don't collide. Whitespace is
  // allowed only around the ':' (trimmed at parse); none inside an identifier part, so the validator
  // and the runtime parser agree on what is accepted.
  private static final Pattern MAP_ENTRY =
      Pattern.compile("[^:\\s]+\\s*:\\s*[^.:\\s]+\\.[^.:\\s]+\\.[^.:\\s]+");

  private static final ConfigDef.Validator TOPIC_TABLE_MAP =
      new ConfigDef.Validator() {
        @Override
        public void ensureValid(String name, Object value) {
          if (value == null) {
            return;
          }
          Set<String> seen = new HashSet<>();
          for (Object o : (List<?>) value) {
            String e = String.valueOf(o).trim();
            if (!MAP_ENTRY.matcher(e).matches()) {
              throw new ConfigException(
                  name, e, "each entry must be '<topic>:<catalog>.<schema>.<table>'");
            }
            String topic = e.substring(0, e.indexOf(':')).trim();
            if (!seen.add(topic)) {
              throw new ConfigException(name, e, "duplicate topic key '" + topic + "'");
            }
          }
        }

        @Override
        public String toString() {
          return "list of <topic>:<catalog>.<schema>.<table>";
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
              "",
              ConfigDef.Importance.HIGH,
              "Bearer token for the Unity Catalog REST API (a PAT or long-lived service-principal "
                  + "token); used when " + AUTH_TYPE + "=" + AUTH_PAT + ". Principal must hold "
                  + "EXTERNAL USE SCHEMA on the target schema.")
          .define(
              AUTH_TYPE,
              ConfigDef.Type.STRING,
              AUTH_PAT,
              ConfigDef.ValidString.in(AUTH_PAT, AUTH_OAUTH_M2M, AUTH_AZURE_ENTRA),
              ConfigDef.Importance.HIGH,
              "How to obtain the bearer token. '" + AUTH_PAT + "': use " + TOKEN + " as-is. '"
                  + AUTH_OAUTH_M2M + "': Databricks service-principal OAuth (client-credentials at "
                  + "{workspace}/oidc/v1/token); the connector mints and refreshes tokens. '"
                  + AUTH_AZURE_ENTRA + "': Microsoft Entra service-principal OAuth for the Azure "
                  + "Databricks resource. The oauth/entra modes refresh on their own before expiry.")
          .define(
              CLIENT_ID,
              ConfigDef.Type.STRING,
              "",
              ConfigDef.Importance.MEDIUM,
              "Service-principal client/application id for " + AUTH_OAUTH_M2M + " / " + AUTH_AZURE_ENTRA + ".")
          .define(
              CLIENT_SECRET,
              ConfigDef.Type.PASSWORD,
              "",
              ConfigDef.Importance.MEDIUM,
              "Service-principal client secret for " + AUTH_OAUTH_M2M + " / " + AUTH_AZURE_ENTRA + ".")
          .define(
              AZURE_TENANT_ID,
              ConfigDef.Type.STRING,
              "",
              ConfigDef.Importance.MEDIUM,
              "Microsoft Entra tenant id; required for " + AUTH_AZURE_ENTRA + ".")
          .define(
              TABLE_NAME_FORMAT,
              ConfigDef.Type.STRING,
              "main.ingestion.${topic}",
              ConfigDef.Importance.HIGH,
              "Default UC name template for topics not matched by topic.to.table. ${topic} is the "
                  + "whole topic; ${topic[N]} is its Nth dot-segment (0-indexed), so a structured "
                  + "topic can route to any catalog.schema.table, e.g. \"bronze.${topic[0]}.${topic[2]}\". "
                  + "Each substituted value must be a valid identifier ([A-Za-z0-9_], <=255 chars) or "
                  + "the routing is rejected -- route dotted topics via ${topic[N]} or pin them with "
                  + "topic.to.table. Must resolve to catalog.schema.table.")
          .define(
              TOPIC_TO_TABLE,
              ConfigDef.Type.LIST,
              Collections.emptyList(),
              TOPIC_TABLE_MAP,
              ConfigDef.Importance.MEDIUM,
              "Explicit per-topic routing that overrides table.name.format. Comma-separated "
                  + "'<topic>:<catalog>.<schema>.<table>' entries, e.g. "
                  + "'orders:main.sales.orders,users:analytics.cdc.users'.")
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
                  + "Drives micro-batch latency; 5s mirrors Zerobus.")
          .define(
              FLUSH_CONCURRENCY,
              ConfigDef.Type.INT,
              1,
              ConfigDef.Range.atLeast(1),
              ConfigDef.Importance.MEDIUM,
              "How many tables to commit concurrently per flush. 1 (default) commits tables serially "
                  + "(today's behavior). The connector is WAN/commit-bound (~2-3% CPU), so committing "
                  + "independent tables in parallel overlaps the round-trips; commits to the SAME table "
                  + "stay serialized (one in flight) to preserve per-partition order + effectively-once.")
          .define(
              AUTO_CREATE_TABLES,
              ConfigDef.Type.BOOLEAN,
              true,
              ConfigDef.Importance.MEDIUM,
              "Create an absent catalog-managed table on first write, deriving the schema from the "
                  + "record (columns nullable). Requires the principal to also hold CREATE TABLE on "
                  + "the target schema. Set false to require pre-created tables.")
          .define(
              WAREHOUSE_ID,
              ConfigDef.Type.STRING,
              "",
              ConfigDef.Importance.MEDIUM,
              "SQL warehouse id used to run CREATE TABLE for " + AUTO_CREATE_TABLES + " (Databricks "
                  + "writes the table's v0) and ALTER TABLE ADD COLUMNS for " + SCHEMA_EVOLUTION
                  + ". Required when auto-creating an absent table or evolving its schema.")
          .define(
              SCHEMA_EVOLUTION,
              ConfigDef.Type.STRING,
              EVOLVE_NONE,
              ConfigDef.ValidString.in(EVOLVE_NONE, EVOLVE_ADD),
              ConfigDef.Importance.MEDIUM,
              "Absorb additive schema changes on a catalog-managed table instead of DLQ-ing the "
                  + "schema-mismatched rows. 'none' (default) keeps the fail-closed poison/DLQ behavior; "
                  + "'add' runs ALTER TABLE ADD COLUMNS (via " + WAREHOUSE_ID + ") for new nullable "
                  + "top-level columns, then appends. Drops/renames/narrowing/type-changes stay poison.");

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
    // schema.evolution runs ALTER TABLE on a SQL warehouse, so it needs one configured. Cross-field;
    // fail at config time rather than on the first additive batch.
    if (!EVOLVE_NONE.equals(schemaEvolution()) && warehouseId().isEmpty()) {
      throw new ConfigException(
          SCHEMA_EVOLUTION + "=" + schemaEvolution() + " requires " + WAREHOUSE_ID
              + " (ALTER TABLE ADD COLUMNS runs on a SQL warehouse).");
    }
    // Each auth type needs a different set of fields; cross-field, so validate here rather than in
    // per-key Validators. Fail at config time, not on the first request.
    switch (authType()) {
      case AUTH_PAT:
        requireSet(TOKEN, token().value());
        break;
      case AUTH_OAUTH_M2M:
        requireSet(CLIENT_ID, getString(CLIENT_ID));
        requireSet(CLIENT_SECRET, clientSecret().value());
        break;
      case AUTH_AZURE_ENTRA:
        requireSet(AZURE_TENANT_ID, getString(AZURE_TENANT_ID));
        requireSet(CLIENT_ID, getString(CLIENT_ID));
        requireSet(CLIENT_SECRET, clientSecret().value());
        break;
      default:
        break; // ValidString already constrains AUTH_TYPE to the known set
    }
  }

  private void requireSet(String key, String value) {
    if (value == null || value.isEmpty()) {
      throw new ConfigException(key + " is required when " + AUTH_TYPE + "=" + authType());
    }
  }

  public String workspaceUrl() {
    return getString(WORKSPACE_URL);
  }

  public Password token() {
    return getPassword(TOKEN);
  }

  public String authType() {
    return getString(AUTH_TYPE);
  }

  public String clientId() {
    return getString(CLIENT_ID);
  }

  public Password clientSecret() {
    return getPassword(CLIENT_SECRET);
  }

  public String azureTenantId() {
    return getString(AZURE_TENANT_ID);
  }

  public String tableNameFormat() {
    return getString(TABLE_NAME_FORMAT);
  }

  /** Explicit topic -&gt; "catalog.schema.table" overrides; empty when unset. */
  public Map<String, String> topicToTable() {
    Map<String, String> m = new LinkedHashMap<>();
    for (String e : getList(TOPIC_TO_TABLE)) {
      int c = e.indexOf(':');
      m.put(e.substring(0, c).trim(), e.substring(c + 1).trim());
    }
    return m;
  }

  public List<String> partitionColumns() {
    return getList(PARTITION_COLUMNS);
  }

  public int flushSize() {
    return getInt(FLUSH_SIZE);
  }

  public int flushConcurrency() {
    return getInt(FLUSH_CONCURRENCY);
  }

  public long flushBytes() {
    return getLong(FLUSH_BYTES);
  }

  public long flushIntervalMs() {
    return getLong(FLUSH_INTERVAL_MS);
  }

  public boolean autoCreateTables() {
    return getBoolean(AUTO_CREATE_TABLES);
  }

  public String warehouseId() {
    return getString(WAREHOUSE_ID);
  }

  public String schemaEvolution() {
    return getString(SCHEMA_EVOLUTION);
  }
}
