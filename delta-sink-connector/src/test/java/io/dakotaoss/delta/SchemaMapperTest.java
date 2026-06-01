package io.dakotaoss.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.dakotaoss.delta.schema.SchemaMapper;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.jupiter.api.Test;

class SchemaMapperTest {

  @Test
  void mapsAllSupportedPrimitives() {
    Schema connect =
        SchemaBuilder.struct()
            .field("i8", Schema.INT8_SCHEMA)
            .field("i16", Schema.INT16_SCHEMA)
            .field("i32", Schema.INT32_SCHEMA)
            .field("i64", Schema.INT64_SCHEMA)
            .field("f32", Schema.FLOAT32_SCHEMA)
            .field("f64", Schema.FLOAT64_SCHEMA)
            .field("b", Schema.BOOLEAN_SCHEMA)
            .field("s", Schema.STRING_SCHEMA)
            .field("by", Schema.BYTES_SCHEMA)
            .build();
    StructType k = SchemaMapper.toKernel(connect);
    assertTrue(k.at(0).getDataType() instanceof ByteType);
    assertTrue(k.at(1).getDataType() instanceof ShortType);
    assertTrue(k.at(2).getDataType() instanceof IntegerType);
    assertTrue(k.at(3).getDataType() instanceof LongType);
    assertTrue(k.at(4).getDataType() instanceof FloatType);
    assertTrue(k.at(5).getDataType() instanceof DoubleType);
    assertTrue(k.at(6).getDataType() instanceof BooleanType);
    assertTrue(k.at(7).getDataType() instanceof StringType);
    assertTrue(k.at(8).getDataType() instanceof BinaryType);
  }

  @Test
  void mapsLogicalTypes() {
    Schema connect =
        SchemaBuilder.struct()
            .field("ts", Timestamp.SCHEMA)
            .field("d", Date.SCHEMA)
            .field("amt", Decimal.schema(4))
            .build();
    StructType k = SchemaMapper.toKernel(connect);
    assertTrue(k.at(0).getDataType() instanceof TimestampType);
    assertTrue(k.at(1).getDataType() instanceof DateType);
    assertTrue(k.at(2).getDataType() instanceof DecimalType);
    assertEquals(4, ((DecimalType) k.at(2).getDataType()).getScale());
  }

  @Test
  void preservesNullability() {
    Schema connect =
        SchemaBuilder.struct()
            .field("req", Schema.INT32_SCHEMA)
            .field("opt", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    StructType k = SchemaMapper.toKernel(connect);
    assertFalse(k.at(0).isNullable());
    assertTrue(k.at(1).isNullable());
  }

  @Test
  void rejectsNonStructTopLevel() {
    assertThrows(IllegalArgumentException.class, () -> SchemaMapper.toKernel(Schema.STRING_SCHEMA));
    assertThrows(IllegalArgumentException.class, () -> SchemaMapper.toKernel(null));
  }

  @Test
  void mapsTopLevelArrayAndMap() {
    Schema connect =
        SchemaBuilder.struct()
            .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .field("attrs", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
            .build();
    StructType k = SchemaMapper.toKernel(connect);
    assertTrue(k.at(0).getDataType() instanceof ArrayType);
    assertTrue(((ArrayType) k.at(0).getDataType()).getElementType() instanceof StringType);
    assertTrue(k.at(1).getDataType() instanceof MapType);
    MapType m = (MapType) k.at(1).getDataType();
    assertTrue(m.getKeyType() instanceof StringType);
    assertTrue(m.getValueType() instanceof IntegerType);
  }
}
