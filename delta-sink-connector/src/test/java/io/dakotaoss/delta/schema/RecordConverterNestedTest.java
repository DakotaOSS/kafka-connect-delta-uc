package io.dakotaoss.delta.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

// nested-envelope conversion + the missing-field / wrong-type / depth guards that feed Redact.
// RecordConverterTest covers leaf conversion + flat null handling; this is the recursive path.
class RecordConverterNestedTest {

  private static SinkRecord rec(Schema vs, Struct value) {
    return new SinkRecord("t", 0, null, null, vs, value, 0L);
  }

  @Test
  void convertsNestedStructIntoChildVectors() {
    Schema after =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    Schema envelope = SchemaBuilder.struct().field("after", after).build();
    StructType kernel = SchemaMapper.toKernel(envelope);

    Struct afterVal = new Struct(after).put("id", 7).put("name", "bob");
    Struct row = new Struct(envelope).put("after", afterVal);

    FilteredColumnarBatch fb = RecordConverter.toBatch(kernel, envelope, Collections.singletonList(rec(envelope, row)));
    ColumnarBatch b = fb.getData();
    assertEquals(1, b.getSize());

    ColumnVector afterCol = b.getColumnVector(0);
    assertTrue(afterCol.getDataType() instanceof StructType);
    assertFalse(afterCol.isNullAt(0));
    assertTrue(afterCol.getChild(0).getDataType() instanceof IntegerType);
    assertTrue(afterCol.getChild(1).getDataType() instanceof StringType);
    assertEquals(7, afterCol.getChild(0).getInt(0));
    assertEquals("bob", afterCol.getChild(1).getString(0));
  }

  @Test
  void nullNestedStructMarksStructNullButKeepsChildVectors() {
    // a Debezium delete leaves "after" null; the struct column reports null per-row while its
    // child vectors still exist (and report null too).
    Schema after = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).optional().build();
    Schema envelope = SchemaBuilder.struct().field("after", after).build();
    StructType kernel = SchemaMapper.toKernel(envelope);

    Struct present = new Struct(envelope).put("after", new Struct(after).put("id", 1));
    Struct absent = new Struct(envelope).put("after", null);
    List<SinkRecord> recs = Arrays.asList(rec(envelope, present), rec(envelope, absent));

    ColumnarBatch b = RecordConverter.toBatch(kernel, envelope, recs).getData();
    ColumnVector afterCol = b.getColumnVector(0);
    assertFalse(afterCol.isNullAt(0));
    assertTrue(afterCol.isNullAt(1));
    assertEquals(1, afterCol.getChild(0).getInt(0));
    assertTrue(afterCol.getChild(0).isNullAt(1));
  }

  @Test
  void nullTopLevelRecordValueYieldsNullColumns() {
    Schema vs = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
    StructType kernel = SchemaMapper.toKernel(vs);
    ColumnarBatch b = RecordConverter.toBatch(kernel, vs, Collections.singletonList(rec(vs, null))).getData();
    assertTrue(b.getColumnVector(0).isNullAt(0));
  }

  @Test
  void rejectsKernelFieldMissingFromConnectSchema() {
    // kernel schema asks for a column the Connect value schema doesn't carry: fail loud, not NPE.
    Schema vs = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
    StructType kernel = new StructType().add("id", IntegerType.INTEGER).add("missing", StringType.STRING);
    Struct row = new Struct(vs).put("id", 1);
    assertThrows(
        DataException.class,
        () -> RecordConverter.toBatch(kernel, vs, Collections.singletonList(rec(vs, row))));
  }

  @Test
  void rejectsNestedKernelFieldMissingFromConnectSchema() {
    Schema after = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
    Schema envelope = SchemaBuilder.struct().field("after", after).build();
    StructType kernel =
        new StructType()
            .add("after", new StructType().add("id", IntegerType.INTEGER).add("missing", StringType.STRING));
    Struct row = new Struct(envelope).put("after", new Struct(after).put("id", 1));
    assertThrows(
        DataException.class,
        () -> RecordConverter.toBatch(kernel, envelope, Collections.singletonList(rec(envelope, row))));
  }

  @Test
  void missingFieldMessageRedactsTheFieldName() {
    // the missing-field name flows into the message through Redact; a SAS-shaped name must be masked.
    Schema vs = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
    StructType kernel = new StructType().add("sig=leakedsas", StringType.STRING);
    Struct row = new Struct(vs).put("id", 1);
    DataException e =
        assertThrows(
            DataException.class,
            () -> RecordConverter.toBatch(kernel, vs, Collections.singletonList(rec(vs, row))));
    assertFalse(e.getMessage().contains("sig=leakedsas"));
    assertTrue(e.getMessage().contains("<redacted>"));
  }

  @Test
  void rejectsNonStructRecordValue() {
    Schema vs = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
    StructType kernel = SchemaMapper.toKernel(vs);
    SinkRecord notAStruct = new SinkRecord("t", 0, null, null, vs, "i am a string", 0L);
    assertThrows(
        DataException.class,
        () -> RecordConverter.toBatch(kernel, vs, Collections.singletonList(notAStruct)));
  }

  @Test
  void rejectsKernelStructMappedToNonStructConnectField() {
    // kernel says "after" is a STRUCT but the Connect schema declares it a scalar; recursing into
    // the struct's children against a non-struct field schema must fail loud, not NPE.
    Schema after = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
    Schema envelope = SchemaBuilder.struct().field("after", after).build();
    StructType kernel = SchemaMapper.toKernel(envelope);
    Schema scalarEnvelope = SchemaBuilder.struct().field("after", Schema.STRING_SCHEMA).build();
    Struct row = new Struct(scalarEnvelope).put("after", "not a struct");
    assertThrows(
        DataException.class,
        () -> RecordConverter.toBatch(kernel, scalarEnvelope, Collections.singletonList(rec(scalarEnvelope, row))));
  }

  @Test
  void rejectsRecordNestedDeeperThanCap() {
    // build matching kernel + Connect schemas 33 structs deep so recursion trips the cap, not a
    // StackOverflow. values can be null; the depth check runs before any value lookup.
    StructType kernelLeaf = new StructType().add("leaf", IntegerType.INTEGER);
    Schema connectLeaf = SchemaBuilder.struct().field("leaf", Schema.INT32_SCHEMA).build();
    StructType kernel = kernelLeaf;
    Schema connect = connectLeaf;
    for (int i = 0; i < 33; i++) {
      kernel = new StructType().add("child", kernel);
      connect = SchemaBuilder.struct().field("child", connect).build();
    }
    final StructType k = kernel;
    final Schema c = connect;
    Struct row = new Struct(c); // child left null; cap fires before we read it
    DataException e =
        assertThrows(
            DataException.class,
            () -> RecordConverter.toBatch(k, c, Collections.singletonList(rec(c, row))));
    assertTrue(e.getMessage().contains("max depth"));
  }
}
