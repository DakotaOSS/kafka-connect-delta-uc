package io.dakotaoss.delta.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.types.StructType;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class RecordConverterTest {

  @Test
  void convertsTimestampToMicros() {
    assertEquals(1_000_000L, RecordConverter.convert(Timestamp.SCHEMA, new Date(1000L)));
  }

  @Test
  void convertsDateToEpochDays() {
    assertEquals(2, RecordConverter.convert(org.apache.kafka.connect.data.Date.SCHEMA, new Date(2L * 86_400_000L)));
  }

  @Test
  void passesThroughDecimalAndNull() {
    BigDecimal d = new BigDecimal("12.34");
    assertEquals(d, RecordConverter.convert(Decimal.schema(2), d));
    assertNull(RecordConverter.convert(Schema.OPTIONAL_STRING_SCHEMA, null));
  }

  @Test
  void buildsColumnarBatchWithNullHandling() {
    Schema vs =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    StructType kernel = SchemaMapper.toKernel(vs);

    Struct r0 = new Struct(vs).put("id", 1).put("name", "alice");
    Struct r1 = new Struct(vs).put("id", 2).put("name", null);
    List<SinkRecord> recs =
        Arrays.asList(
            new SinkRecord("t", 0, null, null, vs, r0, 0L),
            new SinkRecord("t", 0, null, null, vs, r1, 1L));

    FilteredColumnarBatch fb = RecordConverter.toBatch(kernel, vs, recs);
    ColumnarBatch b = fb.getData();
    assertEquals(2, b.getSize());
    assertEquals(1, b.getColumnVector(0).getInt(0));
    assertEquals("alice", b.getColumnVector(1).getString(0));
    assertFalse(b.getColumnVector(1).isNullAt(0));
    assertTrue(b.getColumnVector(1).isNullAt(1));
  }
}
