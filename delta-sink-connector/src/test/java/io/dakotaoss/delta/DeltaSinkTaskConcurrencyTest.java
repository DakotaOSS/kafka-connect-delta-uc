package io.dakotaoss.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.dakotaoss.delta.DeltaSinkTask.TableState;
import io.dakotaoss.delta.model.TableTarget;
import io.dakotaoss.delta.uc.TableResolver;
import io.dakotaoss.delta.uc.UnityCatalogCommitter;
import io.dakotaoss.delta.writer.DeltaKernelWriter;
import io.dakotaoss.delta.writer.EngineProvider;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

/**
 * Fault-injection coverage for the load/failure behaviors that otherwise surface only in prod: the
 * {@code MAX_BUFFERED_RECORDS} backpressure cap, the ~40min vended-SAS TTL re-resolve in stateFor,
 * and a thrown async maintain() on the single-thread executor. Driven through the package-private
 * test seams (the task class is final, so these are injected, not overridden) so neither a million
 * buffered rows nor a live Unity Catalog is needed.
 */
class DeltaSinkTaskConcurrencyTest {

  private static final Schema VALUE_SCHEMA =
      SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();

  // A SAS like a real ABFS 403 carries; the async-maintenance failure path must strip it before logging.
  private static final String SAS_LEAK =
      "Publish failed: abfss://c@acct.dfs.core.windows.net/t?sig=LEAK&se=2026-01-01";

  private static DeltaSinkConfig config() {
    Map<String, String> props = new HashMap<>();
    props.put("name", "delta-sink-test");
    props.put(DeltaSinkConfig.WORKSPACE_URL, "https://example.invalid");
    props.put(DeltaSinkConfig.TOKEN, "unused");
    props.put(DeltaSinkConfig.FLUSH_SIZE, "1000000"); // rows dial effectively off: keep rows buffered
    return new DeltaSinkConfig(props);
  }

  private static SinkRecord rec(String topic, int part, long offset, int id) {
    Struct v = new Struct(VALUE_SCHEMA).put("id", id);
    return new SinkRecord(topic, part, null, null, VALUE_SCHEMA, v, offset);
  }

  private static TableTarget catalogTarget(String topic) {
    return new TableTarget(
        "local." + topic, "file:/unused", "tbl-id", Collections.emptyList(), Collections.emptyMap());
  }

  // ---- backpressure ---------------------------------------------------------------------------

  @Test
  void putThrowsRetriableOncePastTheBufferedCeiling() {
    DeltaSinkTask task =
        new DeltaSinkTask(
            config(),
            t -> new TableTarget("local." + t, "file:/unused", Collections.emptyList(), Collections.emptyMap()),
            EngineProvider.hadoop(),
            new DeltaKernelWriter(),
            "delta-sink-test");
    task.setMaxBufferedRecordsForTest(3);

    // fill the buffer to the cap; flush.size is high so nothing drains
    task.put(List.of(rec("orders", 0, 0L, 1), rec("orders", 0, 1L, 2), rec("orders", 0, 2L, 3)));

    // one more record would push past the cap -> backpressure, not heap growth
    RetriableException ex =
        assertThrows(RetriableException.class, () -> task.put(List.of(rec("orders", 0, 3L, 4))));
    assertTrue(ex.getMessage().contains("cap 3"), ex.getMessage());
    task.stop();
  }

  @Test
  void putUpToTheCeilingDoesNotThrow() {
    DeltaSinkTask task =
        new DeltaSinkTask(
            config(),
            t -> new TableTarget("local." + t, "file:/unused", Collections.emptyList(), Collections.emptyMap()),
            EngineProvider.hadoop(),
            new DeltaKernelWriter(),
            "delta-sink-test");
    task.setMaxBufferedRecordsForTest(2);
    // exactly at the cap is allowed; only crossing it trips backpressure
    task.put(List.of(rec("orders", 0, 0L, 1), rec("orders", 0, 1L, 2)));
    task.stop();
  }

  // ---- credential-TTL re-resolve --------------------------------------------------------------

  // Writer that bypasses the real Delta protocol: appendToSnapshot returns a stub commit result so the
  // task takes the catalog-managed branch and submits maintain(), without touching a filesystem table.
  private static class StubCatalogWriter extends DeltaKernelWriter {
    @Override
    public TransactionCommitResult appendToSnapshot(
        Engine engine, Snapshot snapshot, FilteredColumnarBatch batch, String appId, long version) {
      return new TransactionCommitResult(version, List.of(), null, Optional.empty());
    }

    @Override
    public void maintain(Engine engine, TransactionCommitResult result) {
      // no-op: the re-resolve test isn't about maintenance
    }
  }

  // A real committer just to make committer != null in the fake state; its ctor opens no socket and
  // the flush path never invokes it (the stub writer stands in for the commit).
  private static UnityCatalogCommitter inertCommitter() {
    return new UnityCatalogCommitter(
        "https://example.invalid", "t", "tbl-id", "file:/unused", new Configuration());
  }

  // Builds a catalog-managed state (committer != null) stamped with the task's injected clock, and
  // counts how many times it was (re)built so the TTL-staleness check is observable.
  private static Function<String, TableState> countingBuilder(
      DeltaSinkTask task, AtomicInteger builds) {
    Engine engine = EngineProvider.hadoop().engineFor(catalogTarget("orders"));
    return topic -> {
      builds.incrementAndGet();
      return new TableState(catalogTarget(topic), engine, inertCommitter(), null, task.clockForTest());
    };
  }

  @Test
  void staleCatalogStateReResolvesPastRefreshTtl() {
    AtomicLong now = new AtomicLong(0);
    DeltaSinkTask task =
        new DeltaSinkTask(config(), t -> null, EngineProvider.hadoop(), new StubCatalogWriter(), "delta-sink-test");
    task.setClockForTest(now::get);
    AtomicInteger builds = new AtomicInteger();
    task.setStateBuilderForTest(countingBuilder(task, builds));

    // first commit resolves + caches state
    task.put(List.of(rec("orders", 0, 0L, 1)));
    task.preCommit(Collections.emptyMap());
    assertEquals(1, builds.get());

    // still inside the TTL -> reuse the cached state (no re-resolve)
    now.set(40 * 60 * 1000L - 1);
    task.put(List.of(rec("orders", 0, 1L, 2)));
    task.preCommit(Collections.emptyMap());
    assertEquals(1, builds.get());

    // crossing the ~40min TTL -> drop the cached vended SAS and re-resolve before it can expire
    now.set(40 * 60 * 1000L);
    task.put(List.of(rec("orders", 0, 2L, 3)));
    task.preCommit(Collections.emptyMap());
    assertEquals(2, builds.get());
    task.stop();
  }

  // ---- async-maintenance failure --------------------------------------------------------------

  // Same catalog-managed plumbing, but maintain() throws an exception carrying a SAS, as a real ABFS
  // publish failure would.
  private static final class FailingMaintainWriter extends StubCatalogWriter {
    @Override
    public void maintain(Engine engine, TransactionCommitResult result) {
      throw new RuntimeException(SAS_LEAK);
    }
  }

  @Test
  void asyncMaintenanceFailureIsSwallowedAndRedacted() {
    DeltaSinkTask task =
        new DeltaSinkTask(
            config(), t -> null, EngineProvider.hadoop(), new FailingMaintainWriter(), "delta-sink-test");
    Engine engine = EngineProvider.hadoop().engineFor(catalogTarget("orders"));
    task.setStateBuilderForTest(
        topic -> new TableState(catalogTarget(topic), engine, inertCommitter(), null, task.clockForTest()));
    AtomicReference<String> logged = new AtomicReference<>();
    task.setAsyncMaintenanceErrorSinkForTest(logged::set);

    task.put(List.of(rec("orders", 0, 0L, 1)));
    // commit succeeds despite the doomed maintain(): the exception is swallowed off the commit path
    Map<TopicPartition, OffsetAndMetadata> safe = task.preCommit(Collections.emptyMap());
    assertEquals(1L, safe.get(new TopicPartition("orders", 0)).offset());

    task.stop(); // drains the maintenance executor, so the submitted task has run + logged by now

    String text = logged.get();
    assertTrue(text != null && text.contains("orders"), "failure should be logged for the topic: " + text);
    assertFalse(text.contains("sig=LEAK"), "SAS must be redacted before logging: " + text);
    assertFalse(text.contains("acct.dfs.core.windows.net"), "storage host must be redacted: " + text);

    // the task stays healthy: another batch still commits and advances the offset
    DeltaSinkTask healthy =
        new DeltaSinkTask(
            config(), t -> null, EngineProvider.hadoop(), new StubCatalogWriter(), "delta-sink-test");
    Engine engine2 = EngineProvider.hadoop().engineFor(catalogTarget("orders"));
    healthy.setStateBuilderForTest(
        topic -> new TableState(catalogTarget(topic), engine2, inertCommitter(), null, healthy.clockForTest()));
    healthy.put(List.of(rec("orders", 0, 1L, 2)));
    Map<TopicPartition, OffsetAndMetadata> safe2 = healthy.preCommit(Collections.emptyMap());
    assertEquals(2L, safe2.get(new TopicPartition("orders", 0)).offset());
    healthy.stop();
  }
}
