package io.dakotaoss.delta.schema;

import io.dakotaoss.delta.data.GenericColumnVector;
import io.dakotaoss.delta.data.GenericColumnarBatch;
import io.dakotaoss.delta.util.Redact;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.Optional;

/**
 * Batch of STRUCT-valued {@link SinkRecord}s -> one Kernel {@link FilteredColumnarBatch}.
 *
 * <p>Applies the physical encoding Kernel expects: timestamps as epoch-micros, dates as epoch-days,
 * decimals as {@link java.math.BigDecimal}. Nested STRUCTs recurse, so a Debezium envelope maps to
 * nested struct columns.
 */
public final class RecordConverter {

  private static final long MILLIS_PER_DAY = 86_400_000L;
  // Cap struct recursion so a hostile deeply-nested envelope can't StackOverflow.
  private static final int MAX_DEPTH = 32;

  private RecordConverter() {}

  public static FilteredColumnarBatch toBatch(
      StructType kernelSchema, Schema connectValueSchema, List<SinkRecord> records) {

    int rowCount = records.size();
    int colCount = kernelSchema.length();
    ColumnVector[] columns = new ColumnVector[colCount];

    // Cast each row once, not once per column: the inner loop ran asStruct() colCount times per row.
    Struct[] rows = new Struct[rowCount];
    for (int r = 0; r < rowCount; r++) {
      rows[r] = asStruct(records.get(r).value());
    }

    for (int c = 0; c < colCount; c++) {
      StructField kernelField = kernelSchema.at(c);
      // Missing field would NPE below; surface it as a DataException instead.
      Field connectField = requireField(connectValueSchema, kernelField.getName());
      Object[] values = new Object[rowCount];
      for (int r = 0; r < rowCount; r++) {
        Struct value = rows[r];
        values[r] = value == null ? null : value.get(connectField);
      }
      columns[c] = buildColumn(kernelField.getDataType(), connectField.schema(), values, 1);
    }

    GenericColumnarBatch batch = new GenericColumnarBatch(kernelSchema, columns, rowCount);
    // empty selection vector = select every row
    return new FilteredColumnarBatch(batch, Optional.empty());
  }

  // Leaf columns go through convert(); STRUCT columns recurse into child vectors.
  private static ColumnVector buildColumn(
      DataType kernelType, Schema connectSchema, Object[] values, int depth) {
    if (kernelType instanceof StructType) {
      if (depth > MAX_DEPTH) {
        throw new DataException("Record nesting exceeds max depth " + MAX_DEPTH);
      }
      StructType struct = (StructType) kernelType;
      int rows = values.length;
      boolean[] nulls = new boolean[rows];
      for (int r = 0; r < rows; r++) {
        nulls[r] = values[r] == null;
      }
      ColumnVector[] children = new ColumnVector[struct.length()];
      for (int j = 0; j < struct.length(); j++) {
        StructField childField = struct.at(j);
        // Missing field would NPE below; surface it as a DataException instead.
        Field connectChild = requireField(connectSchema, childField.getName());
        Object[] childValues = new Object[rows];
        for (int r = 0; r < rows; r++) {
          Struct s = asStruct(values[r]);
          childValues[r] = s == null ? null : s.get(connectChild);
        }
        children[j] = buildColumn(childField.getDataType(), connectChild.schema(), childValues, depth + 1);
      }
      return new GenericColumnVector(kernelType, children, nulls);
    }

    Object[] encoded = new Object[values.length];
    for (int r = 0; r < values.length; r++) {
      encoded[r] = convert(connectSchema, values[r]);
    }
    return new GenericColumnVector(kernelType, encoded);
  }

  static Object convert(Schema schema, Object raw) {
    if (raw == null) {
      return null;
    }
    String logical = schema.name();
    if (Timestamp.LOGICAL_NAME.equals(logical)) {
      // Guard the cast: a non-Date value here would ClassCastException.
      if (!(raw instanceof java.util.Date)) {
        throw new DataException(
            "Timestamp field expects java.util.Date, got " + raw.getClass().getName());
      }
      return ((java.util.Date) raw).getTime() * 1_000L; // millis -> micros
    }
    if (Date.LOGICAL_NAME.equals(logical)) {
      if (!(raw instanceof java.util.Date)) {
        throw new DataException(
            "Date field expects java.util.Date, got " + raw.getClass().getName());
      }
      return (int) (((java.util.Date) raw).getTime() / MILLIS_PER_DAY); // days since epoch
    }
    if (Decimal.LOGICAL_NAME.equals(logical)) {
      if (!(raw instanceof java.math.BigDecimal)) {
        throw new DataException(
            "Decimal field expects java.math.BigDecimal, got " + raw.getClass().getName());
      }
      return raw; // already a BigDecimal
    }
    return raw; // primitives map straight through
  }

  // Look up a Connect field, failing loud instead of returning null (which would NPE downstream).
  private static Field requireField(Schema schema, String name) {
    Field f = schema.field(name);
    if (f == null) {
      throw new DataException("Connect value schema is missing field: " + Redact.text(name));
    }
    return f;
  }

  // Guard the (Struct) cast: a non-Struct record value would ClassCastException.
  private static Struct asStruct(Object value) {
    if (value == null || value instanceof Struct) {
      return (Struct) value;
    }
    throw new DataException("Expected STRUCT record value, got " + value.getClass().getName());
  }
}
