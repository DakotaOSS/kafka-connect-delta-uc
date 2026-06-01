package io.dakotaoss.delta.data;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.types.DataType;
import java.math.BigDecimal;

/**
 * Read-only window {@code [start, end)} onto a flat child vector. Backs one array/map row: the
 * collection's elements are stored contiguously across all rows, and a slice exposes one row's run
 * as a 0-based {@link ColumnVector}. Does not own the backing vector, so {@link #close()} is a
 * no-op.
 */
final class SlicedColumnVector implements ColumnVector {

  private final ColumnVector backing;
  private final int start;
  private final int size;

  SlicedColumnVector(ColumnVector backing, int start, int end) {
    this.backing = backing;
    this.start = start;
    this.size = end - start;
  }

  @Override
  public DataType getDataType() {
    return backing.getDataType();
  }

  @Override
  public int getSize() {
    return size;
  }

  @Override
  public void close() {
    // backing vector is shared across slices; the owning column closes it.
  }

  @Override
  public boolean isNullAt(int rowId) {
    return backing.isNullAt(start + rowId);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return backing.getBoolean(start + rowId);
  }

  @Override
  public byte getByte(int rowId) {
    return backing.getByte(start + rowId);
  }

  @Override
  public short getShort(int rowId) {
    return backing.getShort(start + rowId);
  }

  @Override
  public int getInt(int rowId) {
    return backing.getInt(start + rowId);
  }

  @Override
  public long getLong(int rowId) {
    return backing.getLong(start + rowId);
  }

  @Override
  public float getFloat(int rowId) {
    return backing.getFloat(start + rowId);
  }

  @Override
  public double getDouble(int rowId) {
    return backing.getDouble(start + rowId);
  }

  @Override
  public String getString(int rowId) {
    return backing.getString(start + rowId);
  }

  @Override
  public BigDecimal getDecimal(int rowId) {
    return backing.getDecimal(start + rowId);
  }

  @Override
  public byte[] getBinary(int rowId) {
    return backing.getBinary(start + rowId);
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    // struct elements: window the corresponding child the same way.
    return new SlicedColumnVector(backing.getChild(ordinal), start, start + size);
  }

  @Override
  public ArrayValue getArray(int rowId) {
    return backing.getArray(start + rowId);
  }

  @Override
  public MapValue getMap(int rowId) {
    return backing.getMap(start + rowId);
  }
}
