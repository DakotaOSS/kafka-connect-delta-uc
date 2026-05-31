package io.dakotaoss.delta.model;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Resolved write target for a Kafka topic: where the Delta table lives plus any Hadoop/filesystem
 * config (e.g. a UC-vended ABFS SAS token) needed to reach it.
 */
public final class TableTarget {

  private final String fullName; // catalog.schema.table (for logging / appId)
  private final String tablePath; // storage location, e.g. abfss://.../... or file:/tmp/t
  private final String tableId; // UC table id; null for filesystem / non-catalog-managed targets
  private final List<String> partitionColumns;
  // overrides merged into the Engine's Configuration. Carries no secret: the vended SAS lives in
  // VendedSasStore, referenced only by provider-type, so this map is safe even in a config dump.
  private final Map<String, String> hadoopConfig;

  /** Filesystem or non-catalog-managed target (no UC table id). */
  public TableTarget(
      String fullName,
      String tablePath,
      List<String> partitionColumns,
      Map<String, String> hadoopConfig) {
    this(fullName, tablePath, null, partitionColumns, hadoopConfig);
  }

  /** Catalog-managed (Unity Catalog) target carrying the UC {@code tableId}. */
  public TableTarget(
      String fullName,
      String tablePath,
      String tableId,
      List<String> partitionColumns,
      Map<String, String> hadoopConfig) {
    this.fullName = fullName;
    this.tablePath = tablePath;
    this.tableId = tableId;
    this.partitionColumns = partitionColumns == null ? Collections.emptyList() : partitionColumns;
    this.hadoopConfig = hadoopConfig == null ? Collections.emptyMap() : hadoopConfig;
  }

  public String fullName() {
    return fullName;
  }

  /** UC table id for catalog-managed targets, or {@code null} for filesystem targets. */
  public String tableId() {
    return tableId;
  }

  public String tablePath() {
    return tablePath;
  }

  public List<String> partitionColumns() {
    return partitionColumns;
  }

  public Map<String, String> hadoopConfig() {
    return hadoopConfig;
  }
}
