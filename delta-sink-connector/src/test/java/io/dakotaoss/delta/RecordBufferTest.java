package io.dakotaoss.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

/**
 * Per-partition buffer bookkeeping (rows / bytes / first-arrival), extracted from DeltaSinkTask.
 */
class RecordBufferTest {

  private static final Schema S =
      SchemaBuilder.struct()
          .field("id", Schema.OPTIONAL_INT32_SCHEMA)
          .field("name", Schema.OPTIONAL_STRING_SCHEMA)
          .build();

  private static SinkRecord rec(String topic, int part, int off) {
    return new SinkRecord(
        topic, part, null, null, S, new Struct(S).put("id", off).put("name", "n" + off), off);
  }

  private static final TopicPartition TP0 = new TopicPartition("t", 0);
  private static final TopicPartition TP1 = new TopicPartition("t", 1);

  @Test
  void totalRowsSumsAcrossPartitions() {
    RecordBuffer b = new RecordBuffer(false);
    b.add(rec("t", 0, 0), 100);
    b.add(rec("t", 0, 1), 100);
    b.add(rec("t", 1, 0), 100);
    assertEquals(3, b.totalRows());
    assertEquals(2, b.rows(TP0).size());
    assertEquals(1, b.rows(TP1).size());
  }

  @Test
  void bytesAccrueOnlyWhenTrackingEnabled() {
    RecordBuffer off = new RecordBuffer(false);
    off.add(rec("t", 0, 0), 100);
    assertEquals(0L, off.byteSize(TP0), "no byte estimate when flush.bytes is disabled");

    RecordBuffer on = new RecordBuffer(true);
    on.add(rec("t", 0, 0), 100);
    assertTrue(on.byteSize(TP0) > 0, "byte estimate accrues when enabled");
  }

  @Test
  void trippedHonoursSizeAndByteDialsAndIgnoresDisabledOnes() {
    RecordBuffer b = new RecordBuffer(true);
    b.add(rec("t", 0, 0), 100);
    b.add(rec("t", 0, 1), 100);
    // both dials off -> never trips
    assertFalse(b.tripped(TP0, 0, 0));
    // size dial: 2 rows >= 2
    assertTrue(b.tripped(TP0, 2, 0));
    assertFalse(b.tripped(TP0, 3, 0));
    // byte dial: trips at/under the running byte total, not above it
    long bytes = b.byteSize(TP0);
    assertTrue(b.tripped(TP0, 0, bytes));
    assertFalse(b.tripped(TP0, 0, bytes + 1));
  }

  @Test
  void firstArrivalIsStickyUntilCleared() {
    RecordBuffer b = new RecordBuffer(false);
    b.add(rec("t", 0, 0), 100);
    b.add(rec("t", 0, 1), 250); // later arrival must not move the start
    assertEquals(100L, b.startMs(TP0));
    b.clear(TP0);
    b.add(rec("t", 0, 2), 400); // after clear, the next arrival sets a fresh start
    assertEquals(400L, b.startMs(TP0));
  }

  @Test
  void clearEmptiesRowsAndDropsCounters() {
    RecordBuffer b = new RecordBuffer(true);
    b.add(rec("t", 0, 0), 100);
    b.clear(TP0);
    assertTrue(
        b.rows(TP0).isEmpty(), "rows emptied (key kept, matching the old buffers.get(tp).clear())");
    assertEquals(0L, b.byteSize(TP0));
    assertEquals(0, b.totalRows());
  }

  @Test
  void partitionsSnapshotIsSafeToIterateWhileClearing() {
    RecordBuffer b = new RecordBuffer(false);
    b.add(rec("t", 0, 0), 100);
    b.add(rec("t", 1, 0), 100);
    // mirrors flushOnInterval/preCommit: iterate the snapshot, mutate the buffer inside the loop
    for (TopicPartition tp : b.partitions()) {
      b.clear(tp);
    }
    assertEquals(0, b.totalRows());
  }

  @Test
  void clearAllResetsEverything() {
    RecordBuffer b = new RecordBuffer(true);
    b.add(rec("t", 0, 0), 100);
    b.add(rec("t", 1, 0), 100);
    b.clearAll();
    assertEquals(0, b.totalRows());
    assertTrue(b.partitions().isEmpty());
  }
}
