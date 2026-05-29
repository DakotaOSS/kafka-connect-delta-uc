package io.dakotaoss.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.dakotaoss.delta.data.GenericColumnVector;
import io.dakotaoss.delta.data.GenericColumnarBatch;
import io.dakotaoss.delta.model.TableTarget;
import io.dakotaoss.delta.verify.DeltaTableReader;
import io.dakotaoss.delta.writer.DeltaKernelWriter;
import io.dakotaoss.delta.writer.EngineProvider;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Real Delta Kernel write path against a local-filesystem table: create + append, read back, and
 * confirm replay of the same (appId, version) does not duplicate rows.
 */
class DeltaKernelWriterTest {

  private final StructType schema =
      new StructType()
          .add("id", IntegerType.INTEGER, false)
          .add("name", StringType.STRING, true)
          .add("amount", DoubleType.DOUBLE, true);

  private FilteredColumnarBatch batch(Integer[] ids, String[] names, Double[] amounts) {
    ColumnVector[] cols =
        new ColumnVector[] {
          new GenericColumnVector(IntegerType.INTEGER, ids),
          new GenericColumnVector(StringType.STRING, names),
          new GenericColumnVector(DoubleType.DOUBLE, amounts),
        };
    return new FilteredColumnarBatch(
        new GenericColumnarBatch(schema, cols, ids.length), Optional.empty());
  }

  @Test
  void createsAppendsAndReadsBack(@TempDir Path tmp) throws Exception {
    String path = tmp.resolve("orders").toString();
    Engine engine =
        EngineProvider.hadoop()
            .engineFor(new TableTarget("t.t.orders", path, Collections.emptyList(), Collections.emptyMap()));
    DeltaKernelWriter writer = new DeltaKernelWriter();

    // first write also creates the table
    DeltaKernelWriter.Result r1 =
        writer.append(
            engine,
            path,
            schema,
            Collections.emptyList(),
            "app:orders-0",
            10L,
            batch(new Integer[] {1, 2, 3}, new String[] {"a", "b", "c"}, new Double[] {1.5, 2.5, 3.5}));
    assertTrue(r1.applied, "first batch should be applied");
    assertEquals(0L, r1.version, "first commit is table version 0");

    List<Object[]> rows = DeltaTableReader.readRows(engine, path, schema);
    assertEquals(3, rows.size());
    assertEquals(1, rows.get(0)[0]);
    assertEquals("a", rows.get(0)[1]);
    assertEquals(1.5, rows.get(0)[2]);

    // different batch -> appends, no new table
    DeltaKernelWriter.Result r2 =
        writer.append(
            engine,
            path,
            schema,
            Collections.emptyList(),
            "app:orders-0",
            11L,
            batch(new Integer[] {4, 5}, new String[] {"d", "e"}, new Double[] {4.5, 5.5}));
    assertTrue(r2.applied);
    assertEquals(5, DeltaTableReader.countRows(engine, path, schema));
  }

  @Test
  void idempotentReplayDoesNotDuplicate(@TempDir Path tmp) throws Exception {
    String path = tmp.resolve("events").toString();
    Engine engine =
        EngineProvider.hadoop()
            .engineFor(new TableTarget("t.t.events", path, Collections.emptyList(), Collections.emptyMap()));
    DeltaKernelWriter writer = new DeltaKernelWriter();

    FilteredColumnarBatch b =
        batch(new Integer[] {1, 2, 3}, new String[] {"x", "y", "z"}, new Double[] {1.0, 2.0, 3.0});

    DeltaKernelWriter.Result first =
        writer.append(engine, path, schema, Collections.emptyList(), "app:events-0", 100L, b);
    assertTrue(first.applied);
    assertEquals(3, DeltaTableReader.countRows(engine, path, schema));

    // same (appId, version) -> already applied, must not write again
    FilteredColumnarBatch replay =
        batch(new Integer[] {1, 2, 3}, new String[] {"x", "y", "z"}, new Double[] {1.0, 2.0, 3.0});
    DeltaKernelWriter.Result dup =
        writer.append(engine, path, schema, Collections.emptyList(), "app:events-0", 100L, replay);
    assertFalse(dup.applied, "replay of same (appId,version) must be skipped");
    assertEquals(3, DeltaTableReader.countRows(engine, path, schema), "no duplication on replay");
  }
}
