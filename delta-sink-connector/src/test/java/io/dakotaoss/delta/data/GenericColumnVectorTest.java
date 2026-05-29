package io.dakotaoss.delta.data;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.IntegerType;
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
}
