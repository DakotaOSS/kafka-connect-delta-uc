package io.dakotaoss.delta.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;

// nested-struct mapping + hostile-input guards. existing SchemaMapperTest covers the happy primitive
// paths; this fills the recursion / rejection / redaction branches the live tests otherwise own.
class SchemaMapperGuardTest {

  @Test
  void mapsNestedStructToNestedKernelStruct() {
    // shape of a Debezium envelope: top-level struct with a nested "after" record.
    Schema after =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    Schema envelope = SchemaBuilder.struct().field("after", after).build();

    StructType k = SchemaMapper.toKernel(envelope);
    assertTrue(k.at(0).getDataType() instanceof StructType);
    StructType nested = (StructType) k.at(0).getDataType();
    assertTrue(nested.at(0).getDataType() instanceof IntegerType);
    assertTrue(nested.at(1).getDataType() instanceof StringType);
    assertFalse(nested.at(0).isNullable());
    assertTrue(nested.at(1).isNullable());
  }

  @Test
  void mapsNestedNullabilityFromOptionalStructField() {
    Schema source = SchemaBuilder.struct().field("ts", Schema.INT64_SCHEMA).optional().build();
    Schema envelope = SchemaBuilder.struct().field("source", source).build();
    StructType k = SchemaMapper.toKernel(envelope);
    assertTrue(k.at(0).isNullable());
    // children still map through despite the optional wrapper
    assertEquals(1, ((StructType) k.at(0).getDataType()).length());
  }

  @Test
  void arrayElementNullabilityFollowsElementSchema() {
    // optional element schema -> containsNull; required element -> not.
    Schema optElems =
        SchemaBuilder.struct()
            .field("tags", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build())
            .build();
    Schema reqElems =
        SchemaBuilder.struct()
            .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .build();
    assertTrue(((ArrayType) SchemaMapper.toKernel(optElems).at(0).getDataType()).containsNull());
    assertFalse(((ArrayType) SchemaMapper.toKernel(reqElems).at(0).getDataType()).containsNull());
  }

  @Test
  void mapValueNullabilityFollowsValueSchema() {
    Schema optVals =
        SchemaBuilder.struct()
            .field(
                "attrs",
                SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA).build())
            .build();
    Schema reqVals =
        SchemaBuilder.struct()
            .field("attrs", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
            .build();
    assertTrue(((MapType) SchemaMapper.toKernel(optVals).at(0).getDataType()).isValueContainsNull());
    assertFalse(
        ((MapType) SchemaMapper.toKernel(reqVals).at(0).getDataType()).isValueContainsNull());
  }

  @Test
  void mapsArrayOfStructAndMapOfStruct() {
    Schema elem =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    Schema connect =
        SchemaBuilder.struct()
            .field("rows", SchemaBuilder.array(elem).build())
            .field("byId", SchemaBuilder.map(Schema.STRING_SCHEMA, elem).build())
            .build();
    StructType k = SchemaMapper.toKernel(connect);
    ArrayType arr = (ArrayType) k.at(0).getDataType();
    assertTrue(arr.getElementType() instanceof StructType);
    MapType map = (MapType) k.at(1).getDataType();
    assertTrue(map.getKeyType() instanceof StringType);
    assertTrue(map.getValueType() instanceof StructType);
  }

  @Test
  void mapsNestedCollectionCombinations() {
    // array<map<string,int>>, map<string,array<int>>
    Schema connect =
        SchemaBuilder.struct()
            .field(
                "rowsOfMaps",
                SchemaBuilder.array(
                        SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
                    .build())
            .field(
                "listsById",
                SchemaBuilder.map(
                        Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.INT32_SCHEMA).build())
                    .build())
            .build();
    StructType k = SchemaMapper.toKernel(connect);
    assertTrue(((ArrayType) k.at(0).getDataType()).getElementType() instanceof MapType);
    assertTrue(((MapType) k.at(1).getDataType()).getValueType() instanceof ArrayType);
  }

  @Test
  void rejectsCollectionNestedDeeperThanCap() {
    // arrays count toward depth alongside structs: 33 array wrappers trips the cap.
    Schema s = SchemaBuilder.array(Schema.INT32_SCHEMA).build();
    for (int i = 0; i < 33; i++) {
      s = SchemaBuilder.array(s).build();
    }
    Schema deep = SchemaBuilder.struct().field("nest", s).build();
    DataException e = assertThrows(DataException.class, () -> SchemaMapper.toKernel(deep));
    assertTrue(e.getMessage().contains("max depth"));
  }

  @Test
  void rejectsSchemaNestedDeeperThanCap() {
    // 33 levels of STRUCT trips the depth cap before any StackOverflow.
    Schema s = SchemaBuilder.struct().field("leaf", Schema.INT32_SCHEMA).build();
    for (int i = 0; i < 33; i++) {
      s = SchemaBuilder.struct().field("child", s).build();
    }
    final Schema deep = s;
    DataException e = assertThrows(DataException.class, () -> SchemaMapper.toKernel(deep));
    assertTrue(e.getMessage().contains("max depth"));
  }

  @Test
  void allowsSchemaUpToCap() {
    // one shy of the cap maps without throwing.
    Schema s = SchemaBuilder.struct().field("leaf", Schema.INT32_SCHEMA).build();
    for (int i = 0; i < 30; i++) {
      s = SchemaBuilder.struct().field("child", s).build();
    }
    StructType k = SchemaMapper.toKernel(s);
    assertTrue(k.at(0).getDataType() instanceof StructType);
  }

  @Test
  void rejectsDecimalMissingScale() {
    // a Decimal logical type with no parameters at all must fail loud, not NPE.
    Schema bogus = SchemaBuilder.bytes().name(Decimal.LOGICAL_NAME).build();
    Schema envelope = SchemaBuilder.struct().field("amt", bogus).build();
    DataException e = assertThrows(DataException.class, () -> SchemaMapper.toKernel(envelope));
    assertTrue(e.getMessage().contains("scale"));
  }

  @Test
  void rejectsDecimalNonIntegerScaleAndRedactsIt() {
    // a non-integer scale is echoed in the message through Redact; feed it a SAS-shaped value and
    // assert the secret part is masked.
    Schema bogus =
        SchemaBuilder.bytes()
            .name(Decimal.LOGICAL_NAME)
            .parameter(Decimal.SCALE_FIELD, "sig=secretvalue")
            .build();
    Schema envelope = SchemaBuilder.struct().field("amt", bogus).build();
    DataException e = assertThrows(DataException.class, () -> SchemaMapper.toKernel(envelope));
    assertFalse(e.getMessage().contains("sig=secretvalue"));
    assertTrue(e.getMessage().contains("<redacted>"));
  }

  @Test
  void rejectsDecimalScaleOutOfRange() {
    Schema bogus =
        SchemaBuilder.bytes()
            .name(Decimal.LOGICAL_NAME)
            .parameter(Decimal.SCALE_FIELD, "39")
            .build();
    Schema envelope = SchemaBuilder.struct().field("amt", bogus).build();
    DataException e = assertThrows(DataException.class, () -> SchemaMapper.toKernel(envelope));
    assertTrue(e.getMessage().contains("0..38"));
  }
}
