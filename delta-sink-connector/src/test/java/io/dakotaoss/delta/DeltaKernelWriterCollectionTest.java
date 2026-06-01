package io.dakotaoss.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.dakotaoss.delta.model.TableTarget;
import io.dakotaoss.delta.schema.RecordConverter;
import io.dakotaoss.delta.schema.SchemaMapper;
import io.dakotaoss.delta.verify.DeltaTableReader;
import io.dakotaoss.delta.writer.DeltaKernelWriter;
import io.dakotaoss.delta.writer.EngineProvider;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.StructType;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

// proves array/map physical encoding survives the real Kernel write encoder on the offline file://
// path: write a row carrying an array<string> and a map<string,int>, then read it back.
class DeltaKernelWriterCollectionTest {

  @Test
  void roundTripsArrayAndMapColumns(@TempDir Path tmp) throws Exception {
    Schema vs =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .field("attrs", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
            .build();
    StructType schema = SchemaMapper.toKernel(vs);

    Map<String, Integer> attrs = new LinkedHashMap<>();
    attrs.put("a", 10);
    attrs.put("b", 20);
    Struct row =
        new Struct(vs).put("id", 1).put("tags", Arrays.asList("x", "y", "z")).put("attrs", attrs);
    List<SinkRecord> recs =
        Collections.singletonList(new SinkRecord("t", 0, null, null, vs, row, 0L));
    FilteredColumnarBatch batch = RecordConverter.toBatch(schema, vs, recs);

    String path = tmp.resolve("collections").toString();
    Engine engine =
        EngineProvider.hadoop()
            .engineFor(
                new TableTarget(
                    "t.t.collections", path, Collections.emptyList(), Collections.emptyMap()));
    DeltaKernelWriter writer = new DeltaKernelWriter();
    DeltaKernelWriter.Result r =
        writer.append(
            engine, path, "t.t.collections", schema, Collections.emptyList(), "app:c-0", 1L, batch);
    assertTrue(r.applied);

    ColumnarBatch read = DeltaTableReader.readBatch(engine, path, schema);
    assertEquals(1, read.getSize());
    assertEquals(1, read.getColumnVector(0).getInt(0));

    var arr = read.getColumnVector(1).getArray(0);
    assertEquals(3, arr.getSize());
    assertEquals("x", arr.getElements().getString(0));
    assertEquals("z", arr.getElements().getString(2));

    var map = read.getColumnVector(2).getMap(0);
    assertEquals(2, map.getSize());
    // map key/value order is not guaranteed across the encoder; assert by association.
    int found = 0;
    for (int i = 0; i < map.getSize(); i++) {
      String k = map.getKeys().getString(i);
      int v = map.getValues().getInt(i);
      if ("a".equals(k)) {
        assertEquals(10, v);
        found++;
      } else if ("b".equals(k)) {
        assertEquals(20, v);
        found++;
      }
    }
    assertEquals(2, found);
  }
}
