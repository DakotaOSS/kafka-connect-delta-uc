package io.dakotaoss.delta.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.types.IntegerType;
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
  void rejectsTopLevelArrayField() {
    Schema withArray =
        SchemaBuilder.struct()
            .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .build();
    assertThrows(UnsupportedOperationException.class, () -> SchemaMapper.toKernel(withArray));
  }

  @Test
  void rejectsTopLevelMapField() {
    Schema withMap =
        SchemaBuilder.struct()
            .field("attrs", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
            .build();
    assertThrows(UnsupportedOperationException.class, () -> SchemaMapper.toKernel(withMap));
  }

  @Test
  void rejectsArrayNestedInsideStruct() {
    Schema inner =
        SchemaBuilder.struct()
            .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .build();
    Schema envelope = SchemaBuilder.struct().field("after", inner).build();
    assertThrows(UnsupportedOperationException.class, () -> SchemaMapper.toKernel(envelope));
  }

  @Test
  void mapMessageDoesNotLeakRedactableContent() {
    // the rejection message echoes the Connect type, not user content; assert it stays clean even
    // when a SAS-shaped string rides along as a field name.
    Schema withMap =
        SchemaBuilder.struct()
            .field(
                "sig=abc&se=2030",
                SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
            .build();
    UnsupportedOperationException e =
        assertThrows(UnsupportedOperationException.class, () -> SchemaMapper.toKernel(withMap));
    assertFalse(e.getMessage().contains("sig=abc"));
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
