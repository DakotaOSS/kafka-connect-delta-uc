package io.dakotaoss.delta.data;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.DataType;

import java.math.BigDecimal;

/**
 * In-memory {@link ColumnVector} for one column of a write batch. Two shapes:
 *
 * <ul>
 *   <li>Leaf: {@code Object[]} of values already in Kernel's physical representation ({@code Integer},
 *       {@code Long}, {@code Double}, {@code Float}, {@code Short}, {@code Byte}, {@code Boolean},
 *       {@code String}, {@code byte[]}, {@code BigDecimal}); {@code null} is SQL NULL.</li>
 *   <li>Struct: one child vector per field plus a per-row null flag, read via {@link #getChild(int)}.
 *       Carries nested CDC envelopes (e.g. Debezium before/after).</li>
 * </ul>
 *
 * <p>ARRAY and MAP are not handled.
 */
public final class GenericColumnVector implements ColumnVector {

  private final DataType dataType;
  private final Object[] values; // leaf mode; null in struct mode
  private final ColumnVector[] children; // struct mode; null in leaf mode
  private final boolean[] structNulls; // struct mode: per-row null
  private final int size;

  /** Leaf column. */
  public GenericColumnVector(DataType dataType, Object[] values) {
    this.dataType = dataType;
    this.values = values;
    this.children = null;
    this.structNulls = null;
    this.size = values.length;
  }

  /** Struct column: one child vector per field, plus a per-row null flag for the struct itself. */
  public GenericColumnVector(DataType dataType, ColumnVector[] children, boolean[] structNulls) {
    this.dataType = dataType;
    this.values = null;
    this.children = children;
    this.structNulls = structNulls;
    this.size = structNulls.length;
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
  }

  @Override
  public boolean isNullAt(int rowId) {
    return children != null ? structNulls[rowId] : values[rowId] == null;
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    if (children == null) {
      throw new UnsupportedOperationException("getChild called on a non-struct column");
    }
    return children[ordinal];
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
