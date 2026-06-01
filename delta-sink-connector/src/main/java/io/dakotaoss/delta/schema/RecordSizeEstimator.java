package io.dakotaoss.delta.schema;

import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Rough in-memory byte estimate of a record value, for the {@code flush.bytes} dial. Not the
 * serialized/Parquet size; only needs to bound buffer growth and keep file sizes sane. Strings
 * count 2 bytes/char, fixed-width primitives their natural width, structs/arrays/maps recurse.
 */
public final class RecordSizeEstimator {

  private RecordSizeEstimator() {}

  public static long estimate(SinkRecord record) {
    return sizeOf(record.value());
  }

  private static long sizeOf(Object value) {
    if (value == null) {
      return 1;
    }
    if (value instanceof Struct) {
      Struct struct = (Struct) value;
      long total = 0;
      for (Field field : struct.schema().fields()) {
        total += sizeOf(struct.get(field));
      }
      return total;
    }
    if (value instanceof List) {
      long total = 0;
      for (Object e : (List<?>) value) {
        total += sizeOf(e);
      }
      return total;
    }
    if (value instanceof Map) {
      long total = 0;
      for (Map.Entry<?, ?> e : ((Map<?, ?>) value).entrySet()) {
        total += sizeOf(e.getKey()) + sizeOf(e.getValue());
      }
      return total;
    }
    if (value instanceof CharSequence) {
      return 2L * ((CharSequence) value).length();
    }
    if (value instanceof byte[]) {
      return ((byte[]) value).length;
    }
    if (value instanceof Long || value instanceof Double || value instanceof java.util.Date) {
      return 8;
    }
    if (value instanceof Integer || value instanceof Float) {
      return 4;
    }
    if (value instanceof Short) {
      return 2;
    }
    if (value instanceof Byte || value instanceof Boolean) {
      return 1;
    }
    if (value instanceof java.math.BigDecimal) {
      return 16;
    }
    return 8;
  }
}
