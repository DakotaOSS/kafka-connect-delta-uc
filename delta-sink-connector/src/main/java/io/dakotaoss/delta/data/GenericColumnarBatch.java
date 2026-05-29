package io.dakotaoss.delta.data;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.types.StructType;

/** In-memory {@link ColumnarBatch} composed of {@link GenericColumnVector} columns. */
public final class GenericColumnarBatch implements ColumnarBatch {

  private final StructType schema;
  private final ColumnVector[] columns;
  private final int size;

  public GenericColumnarBatch(StructType schema, ColumnVector[] columns, int size) {
    this.schema = schema;
    this.columns = columns;
    this.size = size;
  }

  @Override
  public StructType getSchema() {
    return schema;
  }

  @Override
  public ColumnVector getColumnVector(int ordinal) {
    return columns[ordinal];
  }

  @Override
  public int getSize() {
    return size;
  }
}
