package io.dakotaoss.delta.data;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.types.DataType;
import java.math.BigDecimal;

/**
 * In-memory {@link ColumnVector} for one column of a write batch. Four shapes:
 *
 * <ul>
 *   <li>Leaf: {@code Object[]} of values already in Kernel's physical representation ({@code
 *       Integer}, {@code Long}, {@code Double}, {@code Float}, {@code Short}, {@code Byte}, {@code
 *       Boolean}, {@code String}, {@code byte[]}, {@code BigDecimal}); {@code null} is SQL NULL.
 *   <li>Struct: one child vector per field plus a per-row null flag, read via {@link
 *       #getChild(int)}. Carries nested CDC envelopes (e.g. Debezium before/after).
 *   <li>Array: one flat element child vector plus per-row offsets and a per-row null flag; {@link
 *       #getArray(int)} returns an {@link ArrayValue} over the row's element slice.
 *   <li>Map: key + value child vectors plus per-row offsets and a per-row null flag; {@link
 *       #getMap(int)} returns a {@link MapValue} over the row's key/value slices.
 * </ul>
 */
public final class GenericColumnVector implements ColumnVector {

  private final DataType dataType;
  private final Object[] values; // leaf mode; null otherwise
  private final ColumnVector[] children; // struct mode; null otherwise
  private final boolean[] rowNulls; // struct/array/map mode: per-row null
  private final ColumnVector elements; // array mode: flat element child
  private final ColumnVector keys; // map mode: flat key child
  private final ColumnVector mapValues; // map mode: flat value child
  private final int[] offsets; // array/map mode: row r spans [offsets[r], offsets[r+1])
  private final int size;

  /** Leaf column. */
  public GenericColumnVector(DataType dataType, Object[] values) {
    this.dataType = dataType;
    this.values = values;
    this.children = null;
    this.rowNulls = null;
    this.elements = null;
    this.keys = null;
    this.mapValues = null;
    this.offsets = null;
    this.size = values.length;
  }

  /** Struct column: one child vector per field, plus a per-row null flag for the struct itself. */
  public GenericColumnVector(DataType dataType, ColumnVector[] children, boolean[] structNulls) {
    this.dataType = dataType;
    this.values = null;
    this.children = children;
    this.rowNulls = structNulls;
    this.elements = null;
    this.keys = null;
    this.mapValues = null;
    this.offsets = null;
    this.size = structNulls.length;
  }

  private GenericColumnVector(
      DataType dataType,
      ColumnVector elements,
      ColumnVector keys,
      ColumnVector mapValues,
      int[] offsets,
      boolean[] rowNulls) {
    this.dataType = dataType;
    this.values = null;
    this.children = null;
    this.rowNulls = rowNulls;
    this.elements = elements;
    this.keys = keys;
    this.mapValues = mapValues;
    this.offsets = offsets;
    this.size = rowNulls.length;
  }

  /** Array column: flat element vector, per-row offsets (length size+1), per-row null flag. */
  public static GenericColumnVector forArray(
      DataType dataType, ColumnVector elements, int[] offsets, boolean[] rowNulls) {
    return new GenericColumnVector(dataType, elements, null, null, offsets, rowNulls);
  }

  /** Map column: flat key + value vectors, per-row offsets (length size+1), per-row null flag. */
  public static GenericColumnVector forMap(
      DataType dataType,
      ColumnVector keys,
      ColumnVector values,
      int[] offsets,
      boolean[] rowNulls) {
    return new GenericColumnVector(dataType, null, keys, values, offsets, rowNulls);
  }

  @Override
  public DataType getDataType() {
    return dataType;
  }

  @Override
  public int getSize() {
    return size;
  }

  @Override
  public void close() {
    if (children != null) {
      for (ColumnVector child : children) {
        child.close();
      }
    }
    if (elements != null) {
      elements.close();
    }
    if (keys != null) {
      keys.close();
    }
    if (mapValues != null) {
      mapValues.close();
    }
  }

  @Override
  public boolean isNullAt(int rowId) {
    if (rowNulls != null) {
      return rowNulls[rowId];
    }
    return values[rowId] == null;
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    if (children == null) {
      throw new UnsupportedOperationException("getChild called on a non-struct column");
    }
    return children[ordinal];
  }

  @Override
  public ArrayValue getArray(int rowId) {
    if (elements == null) {
      throw new UnsupportedOperationException("getArray called on a non-array column");
    }
    if (rowNulls[rowId]) {
      return null;
    }
    return new GenericArrayValue(elements, offsets[rowId], offsets[rowId + 1]);
  }

  @Override
  public MapValue getMap(int rowId) {
    if (keys == null) {
      throw new UnsupportedOperationException("getMap called on a non-map column");
    }
    if (rowNulls[rowId]) {
      return null;
    }
    return new GenericMapValue(keys, mapValues, offsets[rowId], offsets[rowId + 1]);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return (Boolean) values[rowId];
  }

  @Override
  public byte getByte(int rowId) {
    return ((Number) values[rowId]).byteValue();
  }

  @Override
  public short getShort(int rowId) {
    return ((Number) values[rowId]).shortValue();
  }

  @Override
  public int getInt(int rowId) {
    return ((Number) values[rowId]).intValue();
  }

  @Override
  public long getLong(int rowId) {
    return ((Number) values[rowId]).longValue();
  }

  @Override
  public float getFloat(int rowId) {
    return ((Number) values[rowId]).floatValue();
  }

  @Override
  public double getDouble(int rowId) {
    return ((Number) values[rowId]).doubleValue();
  }

  @Override
  public String getString(int rowId) {
    return (String) values[rowId];
  }

  @Override
  public BigDecimal getDecimal(int rowId) {
    return (BigDecimal) values[rowId];
  }

  @Override
  public byte[] getBinary(int rowId) {
    return (byte[]) values[rowId];
  }
}
