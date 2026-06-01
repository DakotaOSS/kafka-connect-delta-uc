package io.dakotaoss.delta.schema;

import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Additive-only schema diff for catalog-managed appends. Compares the incoming (Connect-mapped) schema
 * against the current Delta table schema and reports the new top-level columns to add, or flags the
 * change breaking so the caller keeps the fail-closed poison/DLQ path.
 *
 * <p>The schema change is applied Databricks-side via {@code ALTER TABLE ... ADD COLUMNS} on a SQL
 * warehouse (see {@code DeltaSinkTask}), not through Kernel's {@code withUpdatedSchema}: that path
 * needs column mapping, which Kernel 4.2.0 cannot even write into. Adding a nullable column via DDL
 * needs no column mapping and the connector already writes to the (non-column-mapping) table fine.
 *
 * <p>Only new <b>nullable, top-level</b> columns are absorbed. A dropped column, an existing-column
 * type change, a nested-struct change, or a non-nullable new column is breaking -- drops/renames/
 * narrowing are DML the append-only bronze sink does not do, and widening is unavailable on the pinned
 * Kernel.
 */
public final class SchemaEvolution {

  /** What the {@code schema.evolution} dial permits. */
  public enum Policy {
    NONE, // no evolution: any schema mismatch -> poison/DLQ (the default)
    ADD; // absorb new nullable top-level columns

    public static Policy from(String s) {
      return "add".equals(s) ? ADD : NONE;
    }
  }

  /** Diff outcome: the new column names to ADD, and whether the change is non-additive (breaking). */
  public static final class Result {
    public final List<String> addedColumns; // empty when nothing to add
    public final boolean breaking; // not additive -> caller routes the rows to the DLQ

    private Result(List<String> addedColumns, boolean breaking) {
      this.addedColumns = addedColumns;
      this.breaking = breaking;
    }

    public boolean changed() {
      return !addedColumns.isEmpty();
    }
  }

  private static final Result BREAKING = new Result(java.util.Collections.emptyList(), true);

  private SchemaEvolution() {}

  /**
   * Diff {@code incoming} against {@code current}. Every existing column must be present in incoming
   * with an unchanged type; otherwise the change is breaking. New incoming columns must be nullable;
   * their names are returned to drive {@code ALTER TABLE ... ADD COLUMNS}.
   */
  public static Result diff(StructType current, StructType incoming) {
    Map<String, StructField> inc = new LinkedHashMap<>();
    for (StructField f : incoming.fields()) {
      inc.put(f.getName(), f);
    }
    Set<String> currentNames = new HashSet<>();
    for (StructField cur : current.fields()) {
      currentNames.add(cur.getName());
      StructField in = inc.get(cur.getName());
      if (in == null || !cur.getDataType().equivalent(in.getDataType())) {
        // existing column dropped from the batch, or its type changed -> not additive.
        return BREAKING;
      }
    }
    List<String> added = new ArrayList<>();
    for (StructField in : incoming.fields()) {
      if (currentNames.contains(in.getName())) {
        continue;
      }
      if (!in.isNullable()) {
        return BREAKING; // a non-nullable new column can't be back-filled onto existing rows
      }
      added.add(in.getName());
    }
    return new Result(added, false);
  }
}
