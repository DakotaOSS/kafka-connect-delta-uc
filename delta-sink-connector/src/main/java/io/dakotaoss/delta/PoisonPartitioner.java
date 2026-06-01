package io.dakotaoss.delta;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Splits a flush buffer into writable rows and poison. The reference schema is the first record
 * with a usable (non-null Struct) value; a row is good only if its value is a Struct on that exact
 * schema -- everything else (tombstone/null, non-Struct, mismatched schema) is poison the caller
 * routes to the DLQ. Pulling the head-scan out of {@code flush()} keeps this security-relevant
 * decision unit-testable on its own: pinning the ref to {@code batch.get(0)} would DLQ a whole
 * buffer behind a poison head.
 */
final class PoisonPartitioner {

  final Schema refSchema; // null when no row in the batch carries a usable value
  final List<SinkRecord> good;
  final List<SinkRecord> poison;

  private PoisonPartitioner(Schema refSchema, List<SinkRecord> good, List<SinkRecord> poison) {
    this.refSchema = refSchema;
    this.good = good;
    this.poison = poison;
  }

  static PoisonPartitioner of(List<SinkRecord> batch) {
    Schema ref = null;
    for (SinkRecord record : batch) {
      if (record.value() instanceof Struct && record.valueSchema() != null) {
        ref = record.valueSchema();
        break;
      }
    }
    List<SinkRecord> good = new ArrayList<>(batch.size());
    List<SinkRecord> poison = new ArrayList<>();
    for (SinkRecord record : batch) {
      if (record.value() instanceof Struct && ref != null && ref.equals(record.valueSchema())) {
        good.add(record);
      } else {
        poison.add(record);
      }
    }
    return new PoisonPartitioner(ref, good, poison);
  }
}
