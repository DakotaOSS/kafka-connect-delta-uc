package io.dakotaoss.delta.uc;

import io.dakotaoss.delta.util.Redact;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

/**
 * Build the SQL column definitions for a {@code CREATE TABLE} from a Connect value schema, so the
 * connector can auto-create a catalog-managed table whose schema matches the records it will write.
 * Type coverage mirrors {@link io.dakotaoss.delta.schema.SchemaMapper}: leaf types + nested STRUCT;
 * ARRAY/MAP are an extension point. All columns are nullable (Kernel enforces nullability on write).
 */
public final class UcColumnMapper {

  private static final int MAX_DEPTH = 32;

  private UcColumnMapper() {}

  /** Comma-separated column defs, e.g. {@code `id` INT, `name` STRING, `source` STRUCT<lsn:BIGINT>}. */
  public static String ddlColumnDefs(Schema valueSchema) {
    if (valueSchema == null || valueSchema.type() != Schema.Type.STRUCT) {
      throw new DataException(
          "auto-create needs a top-level STRUCT value schema, got "
              + (valueSchema == null ? "null" : valueSchema.type()));
    }
    List<String> defs = new ArrayList<>();
    for (Field f : valueSchema.fields()) {
      // mirror the Connect schema's nullability so the created table matches SchemaMapper.toKernel
      // exactly (Kernel rejects a write whose schema differs, including non-null vs nullable).
      defs.add("`" + f.name() + "` " + sqlType(f.schema(), 0) + (f.schema().isOptional() ? "" : " NOT NULL"));
    }
    return String.join(", ", defs);
  }

  private static String sqlType(Schema schema, int depth) {
    if (schema.type() == Schema.Type.STRUCT) {
      if (depth >= MAX_DEPTH) {
        throw new DataException("Schema nesting exceeds max depth " + MAX_DEPTH);
      }
      List<String> parts = new ArrayList<>();
      for (Field f : schema.fields()) {
        parts.add(
            f.name() + ":" + sqlType(f.schema(), depth + 1) + (f.schema().isOptional() ? "" : " NOT NULL"));
      }
      return "STRUCT<" + String.join(",", parts) + ">";
    }
    String logical = schema.name();
    if (Timestamp.LOGICAL_NAME.equals(logical)) {
      return "TIMESTAMP";
    }
    if (Date.LOGICAL_NAME.equals(logical)) {
      return "DATE";
    }
    if (Decimal.LOGICAL_NAME.equals(logical)) {
      return "DECIMAL(38," + scaleOf(schema) + ")";
    }
    switch (schema.type()) {
      case INT8:
        return "TINYINT";
      case INT16:
        return "SMALLINT";
      case INT32:
        return "INT";
      case INT64:
        return "BIGINT";
      case FLOAT32:
        return "FLOAT";
      case FLOAT64:
        return "DOUBLE";
      case BOOLEAN:
        return "BOOLEAN";
      case STRING:
        return "STRING";
      case BYTES:
        return "BINARY";
      default:
        throw new UnsupportedOperationException(
            "Cannot auto-create a column of Connect type " + schema.type()
                + "; ARRAY/MAP are an extension point. Pre-create the table or omit the column.");
    }
  }

  private static int scaleOf(Schema schema) {
    if (schema.parameters() == null || schema.parameters().get(Decimal.SCALE_FIELD) == null) {
      throw new DataException("Decimal schema is missing required scale parameter");
    }
    int scale;
    try {
      scale = Integer.parseInt(schema.parameters().get(Decimal.SCALE_FIELD));
    } catch (NumberFormatException e) {
      throw new DataException("Decimal scale is not an integer: " + Redact.text(schema.parameters().get(Decimal.SCALE_FIELD)));
    }
    if (scale < 0 || scale > 38) {
      throw new DataException("Decimal scale must be 0..38, got " + scale);
    }
    return scale;
  }
}
