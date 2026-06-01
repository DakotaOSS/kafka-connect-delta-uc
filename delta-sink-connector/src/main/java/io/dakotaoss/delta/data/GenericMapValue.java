package io.dakotaoss.delta.data;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;

/** One map-column row: parallel windows over the column's flat key and value vectors. */
final class GenericMapValue implements MapValue {

  private final ColumnVector keys;
  private final ColumnVector values;

  GenericMapValue(ColumnVector keyBacking, ColumnVector valueBacking, int start, int end) {
    this.keys = new SlicedColumnVector(keyBacking, start, end);
    this.values = new SlicedColumnVector(valueBacking, start, end);
  }

  @Override
  public int getSize() {
    return keys.getSize();
  }

  @Override
  public ColumnVector getKeys() {
    return keys;
  }

  @Override
  public ColumnVector getValues() {
    return values;
  }
}
