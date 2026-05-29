package io.dakotaoss.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.dakotaoss.delta.model.TableTarget;
import io.dakotaoss.delta.schema.SchemaMapper;
import io.dakotaoss.delta.uc.TableResolver;
import io.dakotaoss.delta.verify.DeltaTableReader;
import io.dakotaoss.delta.writer.DeltaKernelWriter;
import io.dakotaoss.delta.writer.EngineProvider;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.StructType;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DeltaSinkTaskTest {

  private static final Schema VALUE_SCHEMA =
      SchemaBuilder.struct()
          .field("id", Schema.INT32_SCHEMA)
          .field("name", Schema.OPTIONAL_STRING_SCHEMA)
          .field("amount", Schema.FLOAT64_SCHEMA)
          .build();

  private static final class LocalTableResolver implements TableResolver {
    private final Path baseDir;

    LocalTableResolver(Path baseDir) {
      this.baseDir = baseDir;
    }

    @Override
    public TableTarget resolve(String topic) {
      return new TableTarget(
          "local." + topic, baseDir.resolve(topic).toString(), Collections.emptyList(), Collections.emptyMap());
    }
  }

  private DeltaSinkTask task(Path tmp, int flushSize) {
    Map<String, String> props = new HashMap<>();
    props.put("name", "delta-sink-test");
    props.put(DeltaSinkConfig.WORKSPACE_URL, "https://example.invalid");
    props.put(DeltaSinkConfig.TOKEN, "unused");
    props.put(DeltaSinkConfig.FLUSH_SIZE, Integer.toString(flushSize));
    return new DeltaSinkTask(
        new DeltaSinkConfig(props),
        new LocalTableResolver(tmp),
        EngineProvider.hadoop(),
        new DeltaKernelWriter(),
        "delta-sink-test");
  }

  private SinkRecord rec(String topic, int part, long offset, int id, String name, double amt) {
    Struct v = new Struct(VALUE_SCHEMA).put("id", id).put("name", name).put("amount", amt);
    return new SinkRecord(topic, part, null, null, VALUE_SCHEMA, v, offset);
  }

  private long count(Path tmp, String topic) throws Exception {
    StructType schema = SchemaMapper.toKernel(VALUE_SCHEMA);
    Engine engine =
        EngineProvider.hadoop()
            .engineFor(new TableTarget("x", tmp.resolve(topic).toString(), Collections.emptyList(), Collections.emptyMap()));
    return DeltaTableReader.countRows(engine, tmp.resolve(topic).toString(), schema);
  }

  @Test
  void preCommitFlushesAndReturnsNextOffset(@TempDir Path tmp) throws Exception {
    DeltaSinkTask task = task(tmp, 1000);
    task.put(List.of(rec("orders", 0, 0L, 1, "a", 1.5), rec("orders", 0, 1L, 2, "b", 2.5), rec("orders", 0, 2L, 3, "c", 3.5)));
    Map<TopicPartition, OffsetAndMetadata> safe = task.preCommit(Collections.emptyMap());
    assertEquals(3L, safe.get(new TopicPartition("orders", 0)).offset());
    assertEquals(3, count(tmp, "orders"));
    task.stop();
  }

  @Test
  void opportunisticFlushWhenBufferReachesFlushSize(@TempDir Path tmp) throws Exception {
    DeltaSinkTask task = task(tmp, 2);
    // 2 records hit flush.size inside put(), so data lands before preCommit
    task.put(List.of(rec("metrics", 0, 0L, 1, "a", 1.0), rec("metrics", 0, 1L, 2, "b", 2.0)));
    assertEquals(2, count(tmp, "metrics"));
    Map<TopicPartition, OffsetAndMetadata> safe = task.preCommit(Collections.emptyMap());
    assertEquals(2L, safe.get(new TopicPartition("metrics", 0)).offset());
    task.stop();
  }

  @Test
  void multiplePartitionsWriteToSameTable(@TempDir Path tmp) throws Exception {
    DeltaSinkTask task = task(tmp, 1000);
    List<SinkRecord> recs = new ArrayList<>();
    recs.add(rec("sales", 0, 0L, 1, "a", 1.0));
    recs.add(rec("sales", 0, 1L, 2, "b", 2.0));
    recs.add(rec("sales", 1, 0L, 3, "c", 3.0));
    task.put(recs);
    Map<TopicPartition, OffsetAndMetadata> safe = task.preCommit(Collections.emptyMap());
    assertEquals(2L, safe.get(new TopicPartition("sales", 0)).offset());
    assertEquals(1L, safe.get(new TopicPartition("sales", 1)).offset());
    assertEquals(3, count(tmp, "sales"));
    task.stop();
  }

  @Test
  void emptyPreCommitReturnsEmpty(@TempDir Path tmp) {
    DeltaSinkTask task = task(tmp, 1000);
    assertTrue(task.preCommit(Collections.emptyMap()).isEmpty());
    task.stop();
  }

  @Test
  void schemalessRecordIsRejected(@TempDir Path tmp) {
    DeltaSinkTask task = task(tmp, 1000);
    SinkRecord schemaless = new SinkRecord("raw", 0, null, null, null, "just-a-string", 0L);
    task.put(List.of(schemaless));
    assertThrows(ConnectException.class, () -> task.preCommit(Collections.emptyMap()));
    task.stop();
  }

  @Test
  void byteDialFlushesBeforeRowThreshold(@TempDir Path tmp) throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("name", "delta-sink-test");
    props.put(DeltaSinkConfig.WORKSPACE_URL, "https://example.invalid");
    props.put(DeltaSinkConfig.TOKEN, "unused");
    props.put(DeltaSinkConfig.FLUSH_SIZE, "100000"); // rows dial effectively off
    props.put(DeltaSinkConfig.FLUSH_BYTES, "40"); // tiny byte threshold -> flush within put()
    DeltaSinkTask task =
        new DeltaSinkTask(
            new DeltaSinkConfig(props),
            new LocalTableResolver(tmp),
            EngineProvider.hadoop(),
            new DeltaKernelWriter(),
            "delta-sink-test");
    task.put(
        List.of(
            rec("bytes", 0, 0L, 1, "alpha", 1.0),
            rec("bytes", 0, 1L, 2, "bravo", 2.0),
            rec("bytes", 0, 2L, 3, "charlie", 3.0)));
    // ~22 bytes/record, so the 40-byte dial trips mid-put, before the row threshold
    assertTrue(count(tmp, "bytes") >= 1, "byte dial should flush before flush.size is reached");
    task.preCommit(Collections.emptyMap());
    assertEquals(3, count(tmp, "bytes"));
    task.stop();
  }
}
