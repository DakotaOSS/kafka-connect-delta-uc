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
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
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
 * Multi-table throughput + resource benchmark: writes to N catalog-managed tables concurrently from a
 * single JVM, the way one connector serving many topics on the same storage account does. With the
 * per-host {@code VendedSasTokenProvider} this shares one cached FileSystem per host (no JVM-global
 * FS-cache disable), so it directly stresses that path under concurrency.
 *
 * <p>Reports aggregate + per-table rows/s and commit-latency percentiles alongside the in-JVM resource
 * view (peak heap, avg/peak process CPU, peak threads) from {@link BenchResourceSampler}; the run
 * script also captures the container (cgroup) view via {@code docker stats}.
 *
 * <p>Env: {@code DATABRICKS_HOST}, {@code DATABRICKS_TOKEN}, {@code BENCH_TABLES} (comma-separated
 * {@code catalog.schema.table}, all the flat CDC schema below), {@code BENCH_ROWS} (rows per table),
 * {@code BENCH_BATCH} (rows per commit, default 100000).
 */
class MultiTableBenchmarkTest {

  private static final int POOL = 10_000;

  // flat post-ExtractNewRecordState CDC row (matches teck_testing.cdc.bench / customers_flat)
  private static final Schema ROW =
      SchemaBuilder.struct()
          .name("dakota.cdc.flat")
          .field("id", Schema.OPTIONAL_INT32_SCHEMA)
          .field("name", Schema.OPTIONAL_STRING_SCHEMA)
          .field("email", Schema.OPTIONAL_STRING_SCHEMA)
          .field("op", Schema.OPTIONAL_STRING_SCHEMA)
          .field("source_ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
          .field("lsn", Schema.OPTIONAL_INT64_SCHEMA)
          .build();

  private static final class Result {
    final String table;
    final long rows;
    final int commits;
    final List<Long> commitNanos;

    Result(String table, long rows, int commits, List<Long> commitNanos) {
      this.table = table;
      this.rows = rows;
      this.commits = commits;
      this.commitNanos = commitNanos;
    }
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "BENCH_TABLES", matches = ".+")
  void multiTableThroughput() throws Exception {
    String host = System.getenv("DATABRICKS_HOST");
    String token = System.getenv("DATABRICKS_TOKEN");
    String[] tables = System.getenv("BENCH_TABLES").split(",");
    long rowsPerTable = Long.parseLong(System.getenv().getOrDefault("BENCH_ROWS", "1000000"));
    int batchRows = Integer.parseInt(System.getenv().getOrDefault("BENCH_BATCH", "100000"));

    StructType kernelSchema = SchemaMapper.toKernel(ROW);
    SinkRecord[] pool = new SinkRecord[POOL];
    for (int i = 0; i < POOL; i++) {
      pool[i] = flatRecord(i);
    }

    UnityCatalogClient uc = new UnityCatalogClient(host, token);
    // one shared maintenance executor across tables, like DeltaSinkTask
    ExecutorService maintenance = Executors.newSingleThreadExecutor();
    ExecutorService writers = Executors.newFixedThreadPool(tables.length);

    System.out.printf("[BENCH-MT] tables=%d rows/table=%d batch=%d shape=flat%n",
        tables.length, rowsPerTable, batchRows);

    BenchResourceSampler sampler = new BenchResourceSampler(250);
    long startNanos = System.nanoTime();
    List<Future<Result>> futures = new ArrayList<>();
    for (String t : tables) {
      String table = t.trim();
      futures.add(writers.submit((Callable<Result>) () ->
          writeTable(uc, host, token, table, kernelSchema, pool, rowsPerTable, batchRows, maintenance)));
    }

    long totalRows = 0;
    int totalCommits = 0;
    List<Long> allCommitNanos = new CopyOnWriteArrayList<>();
    for (Future<Result> f : futures) {
      Result r = f.get();
      totalRows += r.rows;
      totalCommits += r.commits;
      allCommitNanos.addAll(r.commitNanos);
      double sumS = r.commitNanos.stream().mapToLong(Long::longValue).sum() / 1e9;
      System.out.printf("[BENCH-MT] %-44s %d rows, %d commits, %.0f rows/s (sync)%n",
          r.table, r.rows, r.commits, r.rows / Math.max(sumS, 1e-9));
    }
    double wall = (System.nanoTime() - startNanos) / 1e9;
    writers.shutdown();
    maintenance.shutdown();
    sampler.close();

    List<Long> sorted = new ArrayList<>(allCommitNanos);
    Collections.sort(sorted);
    double p50 = sorted.get((int) (sorted.size() * 0.50)) / 1e6;
    double p99 = sorted.get(Math.min(sorted.size() - 1, (int) (sorted.size() * 0.99))) / 1e6;

    System.out.printf(
        "[BENCH-MT-RESULT] tables=%d total_rows=%d commits=%d wallclock_s=%.2f aggregate_rows_s=%.0f "
            + "per_table_rows_s=%.0f commit_ms_p50=%.0f commit_ms_p99=%.0f peak_heap_mb=%.0f "
            + "avg_cpu_pct=%.0f peak_cpu_pct=%.0f peak_threads=%d%n",
        tables.length, totalRows, totalCommits, wall, totalRows / wall,
        (totalRows / wall) / tables.length, p50, p99,
        sampler.peakHeapMb(), sampler.avgCpuPct(), sampler.peakCpuPct(), sampler.peakThreads());
  }

  private static Result writeTable(
      UnityCatalogClient uc, String host, String token, String fullName, StructType kernelSchema,
      SinkRecord[] pool, long rowsPerTable, int batchRows, ExecutorService maintenance)
      throws Exception {
    UcTableResolver resolver = new UcTableResolver(uc, fullName, Collections.emptyList());
    TableTarget target = resolver.resolve("bench");
    UnityCatalogClient.TableInfo info = uc.getTable(fullName);
    Configuration conf = new Configuration();
    target.hadoopConfig().forEach(conf::set);
    Engine engine = DefaultEngine.create(conf);
    UnityCatalogCommitter committer =
        new UnityCatalogCommitter(host, token, info.tableId, info.storageLocation, conf);
    DeltaKernelWriter writer = new DeltaKernelWriter();
    UnityCatalogCommitter.CatalogState state = committer.catalogState();
    Snapshot snapshot =
        writer.loadCatalogSnapshot(engine, target.tablePath(), committer, state.commits, state.maxVersion);

    List<Long> commitNanos = new ArrayList<>();
    List<Future<?>> pending = new ArrayList<>();
    long written = 0;
    int commits = 0;
    while (written < rowsPerTable) {
      int n = (int) Math.min(batchRows, rowsPerTable - written);
      List<SinkRecord> recs = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        recs.add(pool[(int) ((written + i) % POOL)]);
      }
      FilteredColumnarBatch batch = RecordConverter.toBatch(kernelSchema, ROW, recs);
      long t0 = System.nanoTime();
      TransactionCommitResult result = writer.appendToSnapshot(engine, snapshot, batch, null, 0L);
      commitNanos.add(System.nanoTime() - t0);
      snapshot = result.getPostCommitSnapshot().orElseThrow();
      final TransactionCommitResult toMaintain = result;
      pending.add(maintenance.submit(() -> writer.maintain(engine, toMaintain)));
      written += n;
      commits++;
    }
    for (Future<?> f : pending) {
      f.get();
    }
    return new Result(fullName, written, commits, commitNanos);
  }

  private static SinkRecord flatRecord(int i) {
    String op = (i % 13 == 0) ? "d" : (i % 7 == 0) ? "u" : "c";
    Struct v =
        new Struct(ROW)
            .put("id", i)
            .put("name", "customer_" + i)
            .put("email", "user" + i + "@example.com")
            .put("op", op)
            .put("source_ts_ms", 1_780_000_000_000L + i)
            .put("lsn", (long) i);
    return new SinkRecord("flat", 0, null, null, ROW, v, i);
  }
}
