package io.dakotaoss.delta.data;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class GenericColumnVectorTest {

  @Test
  void numericGettersDelegateToNumber() {
    GenericColumnVector v = new GenericColumnVector(IntegerType.INTEGER, new Object[] {7, null});
    assertEquals(2, v.getSize());
    assertEquals(IntegerType.INTEGER, v.getDataType());
    assertEquals((byte) 7, v.getByte(0));
    assertEquals((short) 7, v.getShort(0));
    assertEquals(7, v.getInt(0));
    assertEquals(7L, v.getLong(0));
    assertEquals(7.0f, v.getFloat(0));
    assertEquals(7.0d, v.getDouble(0));
    assertFalse(v.isNullAt(0));
    assertTrue(v.isNullAt(1));
    v.close(); // no-op; must not throw
  }

  @Test
  void booleanStringDecimalBinaryGetters() {
    assertTrue(new GenericColumnVector(BooleanType.BOOLEAN, new Object[] {true}).getBoolean(0));
    assertEquals("hi", new GenericColumnVector(StringType.STRING, new Object[] {"hi"}).getString(0));
    assertEquals(
        new BigDecimal("1.50"),
        new GenericColumnVector(new DecimalType(10, 2), new Object[] {new BigDecimal("1.50")}).getDecimal(0));
    byte[] bytes = {1, 2, 3};
    assertArrayEquals(
        bytes, new GenericColumnVector(BinaryType.BINARY, new Object[] {bytes}).getBinary(0));
  }

  @Test
  void arrayShapeSlicesElementsPerRow() {
    // two rows: [a,b] then [c]; one flat element vector + offsets {0,2,3}.
    ColumnVector elements =
        new GenericColumnVector(StringType.STRING, new Object[] {"a", "b", "c"});
    GenericColumnVector v =
        GenericColumnVector.forArray(
            new ArrayType(StringType.STRING, true),
            elements,
            new int[] {0, 2, 3},
            new boolean[] {false, false});
    assertEquals(2, v.getSize());
    assertTrue(v.getDataType() instanceof ArrayType);
    ArrayValue a0 = v.getArray(0);
    assertEquals(2, a0.getSize());
    assertEquals("a", a0.getElements().getString(0));
    assertEquals("b", a0.getElements().getString(1));
    ArrayValue a1 = v.getArray(1);
    assertEquals(1, a1.getSize());
    assertEquals("c", a1.getElements().getString(0));
  }

  @Test
  void nullArrayRowReturnsNullValue() {
    ColumnVector elements = new GenericColumnVector(StringType.STRING, new Object[] {"a"});
    GenericColumnVector v =
        GenericColumnVector.forArray(
            new ArrayType(StringType.STRING, true),
            elements,
            new int[] {0, 0, 1},
            new boolean[] {true, false});
    assertTrue(v.isNullAt(0));
    assertNull(v.getArray(0));
    assertEquals(1, v.getArray(1).getSize());
  }

  @Test
  void mapShapeSlicesKeysAndValuesPerRow() {
    ColumnVector keys = new GenericColumnVector(StringType.STRING, new Object[] {"x", "y", "z"});
    ColumnVector values = new GenericColumnVector(IntegerType.INTEGER, new Object[] {1, 2, 3});
    GenericColumnVector v =
        GenericColumnVector.forMap(
            new MapType(StringType.STRING, IntegerType.INTEGER, true),
            keys,
            values,
            new int[] {0, 2, 3},
            new boolean[] {false, false});
    MapValue m0 = v.getMap(0);
    assertEquals(2, m0.getSize());
    assertEquals("x", m0.getKeys().getString(0));
    assertEquals(1, m0.getValues().getInt(0));
    assertEquals("y", m0.getKeys().getString(1));
    MapValue m1 = v.getMap(1);
    assertEquals(1, m1.getSize());
    assertEquals("z", m1.getKeys().getString(0));
    assertEquals(3, m1.getValues().getInt(0));
  }

  @Test
  void nullMapRowReturnsNullValue() {
    ColumnVector keys = new GenericColumnVector(StringType.STRING, new Object[] {"x"});
    ColumnVector values = new GenericColumnVector(IntegerType.INTEGER, new Object[] {1});
    GenericColumnVector v =
        GenericColumnVector.forMap(
            new MapType(StringType.STRING, IntegerType.INTEGER, true),
            keys,
            values,
            new int[] {0, 0, 1},
            new boolean[] {true, false});
    assertTrue(v.isNullAt(0));
    assertNull(v.getMap(0));
    assertEquals(1, v.getMap(1).getSize());
  }
}
