package io.dakotaoss.delta;

import io.dakotaoss.delta.model.TableTarget;
import io.dakotaoss.delta.schema.RecordConverter;
import io.dakotaoss.delta.schema.SchemaMapper;
import io.dakotaoss.delta.uc.UcTableResolver;
import io.dakotaoss.delta.uc.UnityCatalogClient;
import io.dakotaoss.delta.uc.UnityCatalogCommitter;
import io.dakotaoss.delta.writer.DeltaKernelWriter;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Throughput + commit-latency benchmark against a managed catalog-managed table, using a Debezium
 * envelope payload (nested before/after/source structs) run through {@link SchemaMapper} /
 * {@link RecordConverter}.
 *
 * <p>Snapshot is loaded once and the in-memory post-commit snapshot is reused for the next append, so
 * there is no per-commit log re-read. Backfill + checkpoint run async, off the latency path; safe
 * because UC already holds the ratified commit. Reported per-commit latency is the synchronous
 * write+ratify a producer waits on.
 *
 * <p>Env: DATABRICKS_HOST, DATABRICKS_TOKEN, BENCH_TABLE (envelope schema, catalogManaged),
 * BENCH_ROWS, BENCH_BATCH.
 */
class BenchmarkTest {

  private static final int POOL = 10_000;
  private static final long TS_BASE = 1_780_000_000_000L;

  private static final Schema ROW =
      SchemaBuilder.struct()
          .name("dakota.customers.Row")
          .optional()
          .field("id", Schema.OPTIONAL_INT32_SCHEMA)
          .field("name", Schema.OPTIONAL_STRING_SCHEMA)
          .field("email", Schema.OPTIONAL_STRING_SCHEMA)
          .build();
  private static final Schema SOURCE =
      SchemaBuilder.struct()
          .name("dakota.cdc.Source")
          .optional()
          .field("db", Schema.OPTIONAL_STRING_SCHEMA)
          .field("table_name", Schema.OPTIONAL_STRING_SCHEMA)
          .field("lsn", Schema.OPTIONAL_INT64_SCHEMA)
          .build();
  private static final Schema ENVELOPE =
      SchemaBuilder.struct()
          .name("dakota.customers.Envelope")
          .field("before", ROW)
          .field("after", ROW)
          .field("op", Schema.OPTIONAL_STRING_SCHEMA)
          .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
          .field("source", SOURCE)
          .build();

  @Test
  @EnabledIfEnvironmentVariable(named = "BENCH_ROWS", matches = ".+")
  void benchmark() throws Exception {
    String host = System.getenv("DATABRICKS_HOST");
    String token = System.getenv("DATABRICKS_TOKEN");
    String fullName = System.getenv("BENCH_TABLE");
    long totalRows = Long.parseLong(System.getenv("BENCH_ROWS"));
    int batchRows = Integer.parseInt(System.getenv().getOrDefault("BENCH_BATCH", "1000000"));

    // prebuild a pool of distinct records; batches reference these to keep allocation out of the loop
    SinkRecord[] pool = new SinkRecord[POOL];
    for (int i = 0; i < POOL; i++) {
      pool[i] = envelopeRecord(i);
    }

    StructType kernelSchema = SchemaMapper.toKernel(ENVELOPE);

    UnityCatalogClient uc = new UnityCatalogClient(host, token);
    UcTableResolver resolver = new UcTableResolver(uc, fullName, Collections.emptyList());
    TableTarget target = resolver.resolve("bench");
    UnityCatalogClient.TableInfo info = uc.getTable(fullName);
    Configuration conf = new Configuration();
    target.hadoopConfig().forEach(conf::set);
    conf.set("fs.abfss.impl.disable.cache", System.getenv().getOrDefault("BENCH_FS_CACHE_DISABLE", "false"));
    Engine engine = DefaultEngine.create(conf);
    UnityCatalogCommitter committer =
        new UnityCatalogCommitter(host, token, info.tableId, info.storageLocation, conf);
    DeltaKernelWriter writer = new DeltaKernelWriter();

    System.out.printf("[BENCH] table=%s rows=%d batch=%d shape=debezium-envelope%n", fullName, totalRows, batchRows);

    UnityCatalogCommitter.CatalogState state = committer.catalogState();
    Snapshot snapshot =
        writer.loadCatalogSnapshot(engine, target.tablePath(), committer, state.commits, state.maxVersion);

    ExecutorService maintenance = Executors.newSingleThreadExecutor();
    List<Future<?>> pending = new ArrayList<>();
    List<Long> commitNanos = new ArrayList<>();
    long startNanos = System.nanoTime();
    long written = 0;
    int commits = 0;
    while (written < totalRows) {
      int n = (int) Math.min(batchRows, totalRows - written);
      List<SinkRecord> batchRecs = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        batchRecs.add(pool[(int) ((written + i) % POOL)]);
      }
      FilteredColumnarBatch batch = RecordConverter.toBatch(kernelSchema, ENVELOPE, batchRecs);

      long t0 = System.nanoTime();
      // appId=null: write every batch, no idempotency suppression
      TransactionCommitResult result = writer.appendToSnapshot(engine, snapshot, batch, null, 0L);
      long batchNanos = System.nanoTime() - t0; // sync write + UC ratify; no publish/checkpoint
      snapshot = result.getPostCommitSnapshot().orElseThrow(); // reuse, no log re-read next time
      final TransactionCommitResult toMaintain = result;
      pending.add(maintenance.submit(() -> writer.maintain(engine, toMaintain)));

      commitNanos.add(batchNanos);
      written += n;
      commits++;
      double elapsed = (System.nanoTime() - startNanos) / 1e9;
      System.out.printf(
          "[BENCH] commit %d: +%d rows, sync %.2fs | %d/%d | %.0f rows/s avg%n",
          commits, n, batchNanos / 1e9, written, totalRows, written / elapsed);
    }
    // drain async backfill/checkpoint before reporting
    for (Future<?> f : pending) {
      f.get();
    }
    maintenance.shutdown();

    double secs = (System.nanoTime() - startNanos) / 1e9;
    Collections.sort(commitNanos);
    double minMs = commitNanos.get(0) / 1e6;
    double p50 = commitNanos.get((int) (commits * 0.50)) / 1e6;
    double p99 = commitNanos.get(Math.min(commits - 1, (int) (commits * 0.99))) / 1e6;
    double maxMs = commitNanos.get(commits - 1) / 1e6;
    System.out.printf(
        "[BENCH-RESULT] table=%s rows=%d commits=%d wallclock_s=%.2f rows_per_sec=%.0f "
            + "sync_commit_ms_min=%.0f p50=%.0f p99=%.0f max=%.0f%n",
        fullName, totalRows, commits, secs, totalRows / secs, minMs, p50, p99, maxMs);
  }

  private static SinkRecord envelopeRecord(int i) {
    String op = (i % 13 == 0) ? "d" : (i % 7 == 0) ? "u" : "c";
    Struct after =
        "d".equals(op)
            ? null
            : new Struct(ROW).put("id", i).put("name", "customer_" + i).put("email", "user" + i + "@example.com");
    Struct before =
        "c".equals(op)
            ? null
            : new Struct(ROW).put("id", i).put("name", "old_" + i).put("email", "user" + i + "@example.com");
    Struct source =
        new Struct(SOURCE).put("db", "sales").put("table_name", "customers").put("lsn", (long) i);
    Struct value =
        new Struct(ENVELOPE)
            .put("before", before)
            .put("after", after)
            .put("op", op)
            .put("ts_ms", TS_BASE + i)
            .put("source", source);
    return new SinkRecord("customers", 0, null, null, ENVELOPE, value, i);
  }
}
