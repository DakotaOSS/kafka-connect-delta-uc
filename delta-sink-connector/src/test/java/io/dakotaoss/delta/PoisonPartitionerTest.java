package io.dakotaoss.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

/** The security-relevant poison/DLQ classification, isolated from the task's commit machinery. */
class PoisonPartitionerTest {

  private static final Schema S =
      SchemaBuilder.struct().field("id", Schema.OPTIONAL_INT32_SCHEMA).build();
  private static final Schema OTHER =
      SchemaBuilder.struct()
          .field("id", Schema.OPTIONAL_INT32_SCHEMA)
          .field("x", Schema.OPTIONAL_STRING_SCHEMA)
          .build();

  private static SinkRecord good(int off) {
    return new SinkRecord("t", 0, null, null, S, new Struct(S).put("id", off), off);
  }

  private static SinkRecord tombstone(int off) { // null value (delete tombstone)
    return new SinkRecord("t", 0, null, null, null, null, off);
  }

  private static SinkRecord nonStruct(int off) { // String value, not a Struct
    return new SinkRecord("t", 0, null, null, Schema.STRING_SCHEMA, "hi", off);
  }

  private static SinkRecord mismatch(int off) { // valid Struct but a different schema
    return new SinkRecord("t", 0, null, null, OTHER, new Struct(OTHER).put("id", off), off);
  }

  @Test
  void allGoodRowsKeepOrderAndPickFirstSchema() {
    SinkRecord a = good(0), b = good(1);
    PoisonPartitioner p = PoisonPartitioner.of(List.of(a, b));
    assertSame(S, p.refSchema);
    assertEquals(List.of(a, b), p.good);
    assertTrue(p.poison.isEmpty());
  }

  @Test
  void headPoisonDoesNotRouteTrailingGoodRowsToDlq() {
    // pinning the ref to batch.get(0) would have DLQ'd the whole buffer; scan past the bad head.
    SinkRecord head = tombstone(0), keep = good(1);
    PoisonPartitioner p = PoisonPartitioner.of(List.of(head, keep));
    assertSame(S, p.refSchema);
    assertEquals(List.of(keep), p.good);
    assertEquals(List.of(head), p.poison);
  }

  @Test
  void schemaMismatchIsPoison() {
    SinkRecord ref = good(0), wrong = mismatch(1);
    PoisonPartitioner p = PoisonPartitioner.of(List.of(ref, wrong));
    assertEquals(List.of(ref), p.good);
    assertEquals(List.of(wrong), p.poison);
  }

  @Test
  void nonStructValueIsPoison() {
    SinkRecord ref = good(0), bad = nonStruct(1);
    PoisonPartitioner p = PoisonPartitioner.of(List.of(ref, bad));
    assertEquals(List.of(ref), p.good);
    assertEquals(List.of(bad), p.poison);
  }

  @Test
  void noUsableValueGivesNullRefAndAllPoison() {
    SinkRecord a = tombstone(0), b = nonStruct(1);
    PoisonPartitioner p = PoisonPartitioner.of(List.of(a, b));
    assertNull(p.refSchema);
    assertTrue(p.good.isEmpty());
    assertEquals(List.of(a, b), p.poison);
  }
}
