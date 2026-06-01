package io.dakotaoss.delta.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.types.StructType;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

// array/map conversion: per-row ArrayValue/MapValue backed by flat child vectors + offsets.
// mirrors RecordConverterNestedTest's struct coverage. round-trip through the real Kernel write
// encoder lives in DeltaKernelWriterCollectionTest.
class RecordConverterCollectionTest {

  private static SinkRecord rec(Schema vs, Struct value) {
    return new SinkRecord("t", 0, null, null, vs, value, 0L);
  }

  @Test
  void convertsArrayOfPrimitiveWithOffsetsPerRow() {
    Schema vs =
        SchemaBuilder.struct()
            .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .build();
    StructType kernel = SchemaMapper.toKernel(vs);

    Struct r0 = new Struct(vs).put("tags", Arrays.asList("a", "b"));
    Struct r1 = new Struct(vs).put("tags", Collections.singletonList("c"));
    ColumnarBatch b =
        RecordConverter.toBatch(kernel, vs, Arrays.asList(rec(vs, r0), rec(vs, r1))).getData();

    ColumnVector col = b.getColumnVector(0);
    ArrayValue a0 = col.getArray(0);
    assertEquals(2, a0.getSize());
    assertEquals("a", a0.getElements().getString(0));
    assertEquals("b", a0.getElements().getString(1));
    ArrayValue a1 = col.getArray(1);
    assertEquals(1, a1.getSize());
    assertEquals("c", a1.getElements().getString(0));
  }

  @Test
  void nullAndEmptyArrays() {
    Schema vs =
        SchemaBuilder.struct()
            .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
            .build();
    StructType kernel = SchemaMapper.toKernel(vs);

    Struct rNull = new Struct(vs).put("tags", null);
    Struct rEmpty = new Struct(vs).put("tags", Collections.emptyList());
    ColumnVector col =
        RecordConverter.toBatch(kernel, vs, Arrays.asList(rec(vs, rNull), rec(vs, rEmpty)))
            .getData()
            .getColumnVector(0);

    assertTrue(col.isNullAt(0));
    assertFalse(col.isNullAt(1));
    assertEquals(0, col.getArray(1).getSize());
  }

  @Test
  void nullElementsInsideArray() {
    Schema vs =
        SchemaBuilder.struct()
            .field("tags", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build())
            .build();
    StructType kernel = SchemaMapper.toKernel(vs);

    Struct r0 = new Struct(vs).put("tags", Arrays.asList("a", null, "c"));
    ColumnVector col =
        RecordConverter.toBatch(kernel, vs, Collections.singletonList(rec(vs, r0)))
            .getData()
            .getColumnVector(0);

    ArrayValue a0 = col.getArray(0);
    assertEquals(3, a0.getSize());
    assertEquals("a", a0.getElements().getString(0));
    assertTrue(a0.getElements().isNullAt(1));
    assertEquals("c", a0.getElements().getString(2));
  }

  @Test
  void convertsMapOfStringToPrimitive() {
    Schema vs =
        SchemaBuilder.struct()
            .field("attrs", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
            .build();
    StructType kernel = SchemaMapper.toKernel(vs);

    Map<String, Integer> m = new LinkedHashMap<>();
    m.put("x", 1);
    m.put("y", 2);
    Struct r0 = new Struct(vs).put("attrs", m);
    ColumnVector col =
        RecordConverter.toBatch(kernel, vs, Collections.singletonList(rec(vs, r0)))
            .getData()
            .getColumnVector(0);

    MapValue mv = col.getMap(0);
    assertEquals(2, mv.getSize());
    assertEquals("x", mv.getKeys().getString(0));
    assertEquals(1, mv.getValues().getInt(0));
    assertEquals("y", mv.getKeys().getString(1));
    assertEquals(2, mv.getValues().getInt(1));
  }

  @Test
  void nullAndEmptyMaps() {
    Schema vs =
        SchemaBuilder.struct()
            .field(
                "attrs",
                SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).optional().build())
            .build();
    StructType kernel = SchemaMapper.toKernel(vs);

    Struct rNull = new Struct(vs).put("attrs", null);
    Struct rEmpty = new Struct(vs).put("attrs", Collections.emptyMap());
    ColumnVector col =
        RecordConverter.toBatch(kernel, vs, Arrays.asList(rec(vs, rNull), rec(vs, rEmpty)))
            .getData()
            .getColumnVector(0);

    assertTrue(col.isNullAt(0));
    assertFalse(col.isNullAt(1));
    assertEquals(0, col.getMap(1).getSize());
  }

  @Test
  void nullMapValues() {
    Schema vs =
        SchemaBuilder.struct()
            .field(
                "attrs",
                SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA).build())
            .build();
    StructType kernel = SchemaMapper.toKernel(vs);

    Map<String, Integer> m = new LinkedHashMap<>();
    m.put("present", 9);
    m.put("absent", null);
    Struct r0 = new Struct(vs).put("attrs", m);
    ColumnVector col =
        RecordConverter.toBatch(kernel, vs, Collections.singletonList(rec(vs, r0)))
            .getData()
            .getColumnVector(0);

    MapValue mv = col.getMap(0);
    assertEquals(2, mv.getSize());
    assertEquals(9, mv.getValues().getInt(0));
    assertTrue(mv.getValues().isNullAt(1));
  }

  @Test
  void convertsArrayOfStruct() {
    Schema elem =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    Schema vs = SchemaBuilder.struct().field("rows", SchemaBuilder.array(elem).build()).build();
    StructType kernel = SchemaMapper.toKernel(vs);

    Struct e0 = new Struct(elem).put("id", 1).put("name", "alice");
    Struct e1 = new Struct(elem).put("id", 2).put("name", null);
    Struct r0 = new Struct(vs).put("rows", Arrays.asList(e0, e1));
    ColumnVector col =
        RecordConverter.toBatch(kernel, vs, Collections.singletonList(rec(vs, r0)))
            .getData()
            .getColumnVector(0);

    ArrayValue a0 = col.getArray(0);
    assertEquals(2, a0.getSize());
    ColumnVector elems = a0.getElements();
    assertTrue(elems.getDataType() instanceof StructType);
    assertEquals(1, elems.getChild(0).getInt(0));
    assertEquals("alice", elems.getChild(1).getString(0));
    assertEquals(2, elems.getChild(0).getInt(1));
    assertTrue(elems.getChild(1).isNullAt(1));
  }

  @Test
  void convertsMapOfStringToStruct() {
    Schema val =
        SchemaBuilder.struct()
            .field("city", Schema.STRING_SCHEMA)
            .field("zip", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    Schema vs =
        SchemaBuilder.struct()
            .field("byId", SchemaBuilder.map(Schema.STRING_SCHEMA, val).build())
            .build();
    StructType kernel = SchemaMapper.toKernel(vs);

    Map<String, Struct> m = new LinkedHashMap<>();
    m.put("k1", new Struct(val).put("city", "den").put("zip", "80202"));
    Struct r0 = new Struct(vs).put("byId", m);
    ColumnVector col =
        RecordConverter.toBatch(kernel, vs, Collections.singletonList(rec(vs, r0)))
            .getData()
            .getColumnVector(0);

    MapValue mv = col.getMap(0);
    assertEquals(1, mv.getSize());
    assertEquals("k1", mv.getKeys().getString(0));
    ColumnVector vals = mv.getValues();
    assertTrue(vals.getDataType() instanceof StructType);
    assertEquals("den", vals.getChild(0).getString(0));
    assertEquals("80202", vals.getChild(1).getString(0));
  }

  @Test
  void convertsMapOfStringToArray() {
    Schema vs =
        SchemaBuilder.struct()
            .field(
                "listsById",
                SchemaBuilder.map(
                        Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.INT32_SCHEMA).build())
                    .build())
            .build();
    StructType kernel = SchemaMapper.toKernel(vs);

    Map<String, List<Integer>> m = new LinkedHashMap<>();
    m.put("k", Arrays.asList(7, 8, 9));
    Struct r0 = new Struct(vs).put("listsById", m);
    ColumnVector col =
        RecordConverter.toBatch(kernel, vs, Collections.singletonList(rec(vs, r0)))
            .getData()
            .getColumnVector(0);

    MapValue mv = col.getMap(0);
    assertEquals(1, mv.getSize());
    ArrayValue nested = mv.getValues().getArray(0);
    assertEquals(3, nested.getSize());
    assertEquals(7, nested.getElements().getInt(0));
    assertEquals(9, nested.getElements().getInt(2));
  }

  @Test
  void rejectsNonListArrayValue() {
    // kernel says ARRAY but the row carries a non-List value: fail loud, not ClassCastException.
    Schema vs =
        SchemaBuilder.struct()
            .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .build();
    StructType kernel = SchemaMapper.toKernel(vs);
    Schema bogus = SchemaBuilder.struct().field("tags", Schema.STRING_SCHEMA).build();
    Struct bad = new Struct(bogus).put("tags", "not-a-list");
    assertThrows(
        DataException.class,
        () -> RecordConverter.toBatch(kernel, bogus, Collections.singletonList(rec(bogus, bad))));
  }

  @Test
  void rejectsNonMapValue() {
    Schema vs =
        SchemaBuilder.struct()
            .field("attrs", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
            .build();
    StructType kernel = SchemaMapper.toKernel(vs);
    Schema bogus = SchemaBuilder.struct().field("attrs", Schema.STRING_SCHEMA).build();
    Struct bad = new Struct(bogus).put("attrs", "not-a-map");
    assertThrows(
        DataException.class,
        () -> RecordConverter.toBatch(kernel, bogus, Collections.singletonList(rec(bogus, bad))));
  }

  @Test
  void nullCollectionElementVectorStillReadable() {
    // a null array still exposes a (zero-length) element vector for its row via getElements.
    Schema vs =
        SchemaBuilder.struct()
            .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
            .build();
    StructType kernel = SchemaMapper.toKernel(vs);
    Struct rNull = new Struct(vs).put("tags", null);
    ColumnVector col =
        RecordConverter.toBatch(kernel, vs, Collections.singletonList(rec(vs, rNull)))
            .getData()
            .getColumnVector(0);
    assertTrue(col.isNullAt(0));
    assertNull(col.getArray(0)); // null row -> null ArrayValue
  }
}
