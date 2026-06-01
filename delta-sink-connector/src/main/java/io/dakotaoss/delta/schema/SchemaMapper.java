package io.dakotaoss.delta.schema;

import io.dakotaoss.delta.util.Redact;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

/**
 * Connect value schema (STRUCT) -> Delta Kernel {@link StructType}.
 *
 * <p>Covers primitives + the logical types seen in CDC / time-series ingestion, nested STRUCTs, and
 * ARRAY/MAP collections (recursively).
 */
public final class SchemaMapper {

  // Cap recursion so a hostile deeply-nested schema can't StackOverflow. Structs, arrays, and maps
  // all count toward depth.
  private static final int MAX_DEPTH = 32;

  private SchemaMapper() {}

  public static StructType toKernel(Schema connectSchema) {
    return toKernel(connectSchema, 0);
  }

  private static StructType toKernel(Schema connectSchema, int depth) {
    if (connectSchema == null || connectSchema.type() != Schema.Type.STRUCT) {
      throw new IllegalArgumentException(
          "Top-level value schema must be a STRUCT, got: "
              + (connectSchema == null ? "null" : connectSchema.type()));
    }
    StructType out = new StructType();
    for (Field field : connectSchema.fields()) {
      out = out.add(field.name(), toKernelType(field.schema(), depth), field.schema().isOptional());
    }
    return out;
  }

  static DataType toKernelType(Schema schema) {
    return toKernelType(schema, 0);
  }

  private static DataType toKernelType(Schema schema, int depth) {
    // Logical types are carried by schema.name().
    String logical = schema.name();
    if (logical != null) {
      switch (logical) {
        case Timestamp.LOGICAL_NAME:
          return TimestampType.TIMESTAMP;
        case Date.LOGICAL_NAME:
          return DateType.DATE;
        case Decimal.LOGICAL_NAME:
          // parameters()/scale may be absent or out of range on a malformed schema; fail loud.
          if (schema.parameters() == null || schema.parameters().get(Decimal.SCALE_FIELD) == null) {
            throw new DataException("Decimal schema is missing required scale parameter");
          }
          int scale;
          try {
            scale = Integer.parseInt(schema.parameters().get(Decimal.SCALE_FIELD));
          } catch (NumberFormatException e) {
            throw new DataException(
                "Decimal scale is not an integer: "
                    + Redact.text(schema.parameters().get(Decimal.SCALE_FIELD)));
          }
          if (scale < 0 || scale > 38) {
            throw new DataException("Decimal scale must be 0..38, got " + scale);
          }
          // Connect Decimal carries no precision; use Delta's max (38).
          return new DecimalType(38, scale);
        default:
          break; // not a known logical type; fall through to physical
      }
    }
    switch (schema.type()) {
      case INT8:
        return ByteType.BYTE;
      case INT16:
        return ShortType.SHORT;
      case INT32:
        return IntegerType.INTEGER;
      case INT64:
        return LongType.LONG;
      case FLOAT32:
        return FloatType.FLOAT;
      case FLOAT64:
        return DoubleType.DOUBLE;
      case BOOLEAN:
        return BooleanType.BOOLEAN;
      case STRING:
        return StringType.STRING;
      case BYTES:
        return BinaryType.BINARY;
      case STRUCT:
        if (depth >= MAX_DEPTH) {
          throw new DataException("Schema nesting exceeds max depth " + MAX_DEPTH);
        }
        return toKernel(schema, depth + 1); // nested record, e.g. Debezium before/after/source
      case ARRAY:
        if (depth >= MAX_DEPTH) {
          throw new DataException("Schema nesting exceeds max depth " + MAX_DEPTH);
        }
        // element nullability follows the element schema's optionality.
        return new ArrayType(
            toKernelType(schema.valueSchema(), depth + 1), schema.valueSchema().isOptional());
      case MAP:
        if (depth >= MAX_DEPTH) {
          throw new DataException("Schema nesting exceeds max depth " + MAX_DEPTH);
        }
        // map keys are non-null in Delta; value nullability follows the value schema.
        return new MapType(
            toKernelType(schema.keySchema(), depth + 1),
            toKernelType(schema.valueSchema(), depth + 1),
            schema.valueSchema().isOptional());
      default:
        throw new UnsupportedOperationException(
            "Unsupported Connect type for Delta mapping: " + schema.type());
    }
  }

  /** Single-field mapping. public for tests. */
  public static StructField fieldOf(Field field) {
    return new StructField(field.name(), toKernelType(field.schema()), field.schema().isOptional());
  }
}
