package io.dakotaoss.delta.data;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;

/** One array-column row: a window over the column's flat element vector. */
final class GenericArrayValue implements ArrayValue {

  private final ColumnVector elements;

  GenericArrayValue(ColumnVector backing, int start, int end) {
    this.elements = new SlicedColumnVector(backing, start, end);
  }

  @Override
  public int getSize() {
    return elements.getSize();
  }

  @Override
  public ColumnVector getElements() {
    return elements;
  }
}
