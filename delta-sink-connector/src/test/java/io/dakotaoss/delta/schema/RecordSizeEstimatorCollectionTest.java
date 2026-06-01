package io.dakotaoss.delta.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

// flush.bytes accounting must include array/map payloads, not silently treat them as a flat 8 bytes.
class RecordSizeEstimatorCollectionTest {

  private static SinkRecord rec(Schema vs, Struct value) {
    return new SinkRecord("t", 0, null, null, vs, value, 0L);
  }

  @Test
  void arrayPayloadCountsEachElement() {
    Schema vs =
        SchemaBuilder.struct()
            .field("nums", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
            .build();
    Struct row = new Struct(vs).put("nums", Arrays.asList(1, 2, 3));
    // three INT32 elements at 4 bytes each.
    assertEquals(12, RecordSizeEstimator.estimate(rec(vs, row)));
  }

  @Test
  void mapPayloadCountsKeysAndValues() {
    Schema vs =
        SchemaBuilder.struct()
            .field("m", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
            .build();
    Map<String, Integer> m = new LinkedHashMap<>();
    m.put("ab", 1); // 2-char key -> 4 bytes, int -> 4 bytes
    Struct row = new Struct(vs).put("m", m);
    assertEquals(8, RecordSizeEstimator.estimate(rec(vs, row)));
  }

  @Test
  void largerCollectionEstimatesLarger() {
    Schema vs =
        SchemaBuilder.struct()
            .field("nums", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
            .build();
    Struct small = new Struct(vs).put("nums", Arrays.asList(1));
    Struct big = new Struct(vs).put("nums", Arrays.asList(1, 2, 3, 4, 5));
    assertTrue(RecordSizeEstimator.estimate(rec(vs, big)) > RecordSizeEstimator.estimate(rec(vs, small)));
  }
}
