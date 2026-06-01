package io.dakotaoss.delta;

import io.dakotaoss.delta.auth.CredentialProvider;
import io.dakotaoss.delta.auth.Credentials;
import io.dakotaoss.delta.model.TableTarget;
import io.dakotaoss.delta.schema.RecordConverter;
import io.dakotaoss.delta.schema.RecordSizeEstimator;
import io.dakotaoss.delta.schema.SchemaMapper;
import io.dakotaoss.delta.uc.TableResolver;
import io.dakotaoss.delta.uc.UcTableResolver;
import io.dakotaoss.delta.uc.UnityCatalogClient;
import io.dakotaoss.delta.uc.UnityCatalogCommitter;
import io.dakotaoss.delta.util.Redact;
import io.dakotaoss.delta.writer.DeltaKernelWriter;
import io.dakotaoss.delta.writer.EngineProvider;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Buffers records per topic-partition; commits each buffer as one idempotent Delta transaction.
 *
 * <p>Three flush dials, first to trip wins: {@code flush.size} (rows), {@code flush.bytes} (approx
 * payload, for target file size), {@code flush.interval.ms} (max latency). Size and bytes flush
 * inside {@code put} when a buffer fills; the interval is driven by a scheduler that flushes any
 * non-empty buffer each tick, so rows still commit under light traffic.
 *
 * <p>Catalog-managed (Unity Catalog) tables take the streaming path: load the snapshot once per
 * table, reuse the in-memory post-commit snapshot (no per-commit log re-read), and run
 * backfill/checkpoint off the commit path on a background executor. Filesystem tables commit
 * directly. {@code preCommit} returns offsets only after the Delta commit succeeds; together with
 * the per-partition {@code SetTransaction(appId, offset)} stamp this gives effectively-once delivery
 * across restarts.
 */
public final class DeltaSinkTask extends SinkTask {

  static final String VERSION = "0.1.0";
  private static final Logger LOG = LoggerFactory.getLogger(DeltaSinkTask.class);

  // Hard ceiling on rows buffered across all partitions; past it we apply backpressure (H6) so a
  // stalled flush (e.g. UC outage) cannot grow the heap without bound.
  private static final int MAX_BUFFERED_RECORDS = 1_000_000;
  // Re-resolve a cached catalog-managed table before its vended SAS (~1h TTL) can expire (H5).
  private static final long REFRESH_MS = 40 * 60 * 1000;

  private DeltaSinkConfig config;
  // one provider per task, shared across all tables: a single token (refreshed on its own for the
  // oauth/entra modes) authenticates every UC call this task makes.
  private CredentialProvider credential;
  private TableResolver resolver;
  private EngineProvider engineProvider;
  private DeltaKernelWriter writer;
  private String connectorName;
  private int flushSize;
  private long flushBytes;
  private long flushIntervalMs;

  private final Object lock = new Object();
  private final Map<TopicPartition, List<SinkRecord>> buffers = new HashMap<>();
  private final Map<TopicPartition, Long> bufferStartMs = new HashMap<>();
  private final Map<TopicPartition, Long> bufferBytes = new HashMap<>();
  private final Map<TopicPartition, Long> committed = new HashMap<>();
  private final Map<String, TableState> tableStates = new HashMap<>();

  private ExecutorService maintenance;
  private ScheduledExecutorService flushScheduler;
  // Connect's DLQ hook; null when the test constructor is used or no reporter is configured.
  private ErrantRecordReporter reporter;

  // Test seams. Defaults match prod; tests inject to drive the otherwise only-in-prod backpressure,
  // ~40min TTL re-resolve, and async-maintenance failure paths without 1M rows or a live UC. The
  // class is final, so these are injection points rather than overridable methods.
  private int maxBufferedRecords = MAX_BUFFERED_RECORDS;
  private java.util.function.LongSupplier clock = System::currentTimeMillis;
  // builds a fresh (uncached) write state for a topic; the live impl resolves + opens UC, a test can
  // supply a catalog-managed state without a network round-trip.
  private java.util.function.Function<String, TableState> stateBuilder = this::buildState;
  // redacted async-maintenance failure sink; the redaction happens before the text reaches here.
  private java.util.function.Consumer<String> asyncMaintenanceErrorSink =
      msg -> LOG.warn("Async maintenance failed: {}", msg);

  /** Per-table write state. For catalog-managed tables the snapshot is reused across commits. */
  static final class TableState {
    final TableTarget target;
    final Engine engine;
    final UnityCatalogCommitter committer; // null for filesystem tables
    Snapshot snapshot; // null for filesystem; advanced per commit for catalog-managed
    final long resolvedAtMs; // clock reading at resolve; used to expire vended SAS before TTL (H5)

    TableState(
        TableTarget target,
        Engine engine,
        UnityCatalogCommitter committer,
        Snapshot snapshot,
        long resolvedAtMs) {
      this.target = target;
      this.engine = engine;
      this.committer = committer;
      this.snapshot = snapshot;
      this.resolvedAtMs = resolvedAtMs;
    }
  }

  /** Required by Kafka Connect. */
  public DeltaSinkTask() {}

  /** Test constructor: inject resolver/engine/writer to bypass live Unity Catalog. */
  DeltaSinkTask(
      DeltaSinkConfig config,
      TableResolver resolver,
      EngineProvider engineProvider,
      DeltaKernelWriter writer,
      String connectorName) {
    this.config = config;
    this.credential = Credentials.fromConfig(config);
    this.resolver = resolver;
    this.engineProvider = engineProvider;
    this.writer = writer;
    this.connectorName = connectorName;
    this.flushSize = config.flushSize();
    this.flushBytes = config.flushBytes();
    this.flushIntervalMs = config.flushIntervalMs();
    this.maintenance = Executors.newSingleThreadExecutor();
  }

  /** Test seam: supply the DLQ reporter that {@code start()} would otherwise pull from the context. */
  void injectReporter(ErrantRecordReporter reporter) {
    this.reporter = reporter;
  }

  /** Test seam: shrink the backpressure ceiling so the cap is reachable without millions of rows. */
  void setMaxBufferedRecordsForTest(int cap) {
    this.maxBufferedRecords = cap;
  }

  /** Test seam: drive the SAS-TTL re-resolve clock instead of waiting ~40 min of wall time. */
  void setClockForTest(java.util.function.LongSupplier clock) {
    this.clock = clock;
  }

  /** Test seam: capture the redacted text the async-maintenance failure path would otherwise log. */
  void setAsyncMaintenanceErrorSinkForTest(java.util.function.Consumer<String> sink) {
    this.asyncMaintenanceErrorSink = sink;
  }

  /** Test seam: supply a write-state builder so a catalog-managed flush needs no live UC. */
  void setStateBuilderForTest(java.util.function.Function<String, TableState> builder) {
    this.stateBuilder = builder;
  }

  /** Test seam: clock reading used to stamp a {@link TableState} built outside the live resolver. */
  long clockForTest() {
    return clock.getAsLong();
  }

  @Override
  public String version() {
    return VERSION;
  }

  @Override
  public void start(Map<String, String> props) {
    this.config = new DeltaSinkConfig(props);
    this.connectorName = props.getOrDefault("name", "delta-sink");
    this.flushSize = config.flushSize();
    this.flushBytes = config.flushBytes();
    this.flushIntervalMs = config.flushIntervalMs();
    // One credential provider for the whole task: for oauth/entra it mints and refreshes tokens on
    // its own (before expiry); for pat it reads the config token per request. Either way the UC
    // clients just see a Supplier<String> that always yields a valid token.
    this.credential = Credentials.fromConfig(config);
    UnityCatalogClient uc = new UnityCatalogClient(config.workspaceUrl(), credential);
    this.resolver =
        new UcTableResolver(
            uc, config.tableNameFormat(), config.topicToTable(), config.partitionColumns());
    this.engineProvider = EngineProvider.hadoop();
    this.writer = new DeltaKernelWriter();
    this.maintenance = Executors.newSingleThreadExecutor();
    // capture the errant-record reporter once; context (and the reporter) may be absent
    this.reporter = context == null ? null : context.errantRecordReporter();
    if (flushIntervalMs > 0) {
      this.flushScheduler = Executors.newSingleThreadScheduledExecutor();
      flushScheduler.scheduleWithFixedDelay(
          this::flushOnInterval, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    }
    LOG.info(
        "DeltaSinkTask {} started; flush.size={} flush.bytes={} flush.interval.ms={}",
        connectorName,
        flushSize,
        flushBytes,
        flushIntervalMs);
  }

  @Override
  public void put(java.util.Collection<SinkRecord> records) {
    synchronized (lock) {
      // backpressure: refuse to grow the heap past the ceiling. RetriableException makes Connect
      // pause and re-deliver this same batch once flushes drain the buffers (H6).
      int buffered = 0;
      for (List<SinkRecord> b : buffers.values()) {
        buffered += b.size();
      }
      if (buffered + records.size() > maxBufferedRecords) {
        throw new RetriableException(
            "buffered records " + buffered + " + " + records.size()
                + " would exceed cap " + maxBufferedRecords + "; applying backpressure");
      }
      long now = clock.getAsLong();
      for (SinkRecord record : records) {
        TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
        buffers.computeIfAbsent(tp, k -> new ArrayList<>()).add(record);
        bufferStartMs.putIfAbsent(tp, now);
        if (flushBytes > 0) {
          bufferBytes.merge(tp, RecordSizeEstimator.estimate(record), Long::sum);
        }
      }
      // early flush: size/bytes thresholds commit ahead of the interval tick
      for (TopicPartition tp : new ArrayList<>(buffers.keySet())) {
        boolean bySize = flushSize > 0 && buffers.get(tp).size() >= flushSize;
        boolean byBytes = flushBytes > 0 && bufferBytes.getOrDefault(tp, 0L) >= flushBytes;
        if (bySize || byBytes) {
          flush(tp);
        }
      }
    }
  }

  /** Scheduled tick: flush any non-empty buffer so queued rows commit within flush.interval.ms. */
  private void flushOnInterval() {
    synchronized (lock) {
      try {
        for (TopicPartition tp : new ArrayList<>(buffers.keySet())) {
          flush(tp);
        }
      } catch (Exception e) {
        // redact: the cause can embed a vended SAS
        LOG.error("Scheduled flush failed: {}", Redact.message(e));
      }
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    synchronized (lock) {
      for (TopicPartition tp : new ArrayList<>(buffers.keySet())) {
        flush(tp);
      }
      Map<TopicPartition, OffsetAndMetadata> safe = new HashMap<>();
      for (Map.Entry<TopicPartition, Long> e : committed.entrySet()) {
        safe.put(e.getKey(), new OffsetAndMetadata(e.getValue()));
      }
      return safe;
    }
  }

  /** Commit one topic-partition's buffer as a single Delta transaction. Caller holds {@code lock}. */
  private void flush(TopicPartition tp) {
    List<SinkRecord> batch = buffers.get(tp);
    if (batch == null || batch.isEmpty()) {
      return;
    }
    long lastOffset = batch.get(batch.size() - 1).kafkaOffset();

    // Reference schema = the first record carrying a usable (non-null Struct) value. Pinning it to
    // batch.get(0) would route the whole buffer to the DLQ when a poison/tombstone row sits at the
    // head; scanning for the first good row keeps the valid rows behind it.
    Schema refSchema = null;
    for (SinkRecord record : batch) {
      if (record.value() instanceof Struct && record.valueSchema() != null) {
        refSchema = record.valueSchema();
        break;
      }
    }

    // Partition the buffer: rows with a null/non-Struct value or a schema differing from the
    // reference are poison. reportBad routes them to the DLQ (the commit then advances the offset
    // past them), or fails the task when no DLQ reporter is configured -- the offset never advances
    // over poison silently.
    List<SinkRecord> good = new ArrayList<>(batch.size());
    for (SinkRecord record : batch) {
      boolean ok =
          record.value() instanceof Struct
              && refSchema != null
              && refSchema.equals(record.valueSchema());
      if (ok) {
        good.add(record);
      } else {
        reportBad(record, new ConnectException("poison record: non-Struct/null value or schema mismatch"));
      }
    }

    if (good.isEmpty()) {
      // everything was skipped; still advance/clear so the partition makes progress.
      commitFlushed(tp, lastOffset);
      LOG.warn("Flush for {} dropped all {} records as poison", tp, batch.size());
      return;
    }

    String appId = connectorName + ":" + tp.topic() + "-" + tp.partition();
    TableState st = stateFor(tp.topic());
    try {
      StructType kernelSchema = SchemaMapper.toKernel(refSchema);
      FilteredColumnarBatch data = RecordConverter.toBatch(kernelSchema, refSchema, good);
      if (st.committer != null) {
        // catalog-managed: reuse snapshot, run backfill/checkpoint async
        TransactionCommitResult result =
            writer.appendToSnapshot(st.engine, st.snapshot, data, appId, lastOffset);
        if (result != null) {
          st.snapshot = result.getPostCommitSnapshot().orElse(st.snapshot);
          final TableState fst = st;
          maintenance.submit(
              () -> {
                try {
                  writer.maintain(fst.engine, result);
                } catch (Exception e) {
                  // swallow: UC already holds the ratified commit, so a failed publish/checkpoint is
                  // non-fatal. Redact first: the cause can embed a vended SAS.
                  asyncMaintenanceErrorSink.accept(tp.topic() + ": " + Redact.message(e));
                }
              });
        }
      } else {
        // filesystem table: direct commit
        writer.append(
            st.engine,
            st.target.tablePath(),
            st.target.fullName(),
            kernelSchema,
            st.target.partitionColumns(),
            appId,
            lastOffset,
            data);
      }
    } catch (Exception e) {
      // drop cached state so next flush re-resolves: re-vends creds, reloads snapshot
      tableStates.remove(tp.topic());
      // Redact before the failure leaves our code and drop the raw cause: ABFS IOExceptions embed the
      // request URL incl. the vended SAS, and whatever we hand off persists -- DLQ record headers, and
      // Connect's task-failure log (which prints the whole cause chain). Carry only the redacted text.
      ConnectException safe = new ConnectException("flush failed for " + tp + ": " + Redact.message(e));
      if (reporter != null) {
        // conversion/write failed for this batch as a whole: route its records to the DLQ and skip,
        // rather than crash-looping the task on data we cannot write.
        for (SinkRecord record : good) {
          reporter.report(record, safe);
        }
        commitFlushed(tp, lastOffset);
        LOG.warn("Flush for {} reported {} records to DLQ: {}", tp, good.size(), safe.getMessage());
        return;
      }
      LOG.error("{}", safe.getMessage());
      throw safe;
    }

    commitFlushed(tp, lastOffset);
    LOG.info("Flushed {} records for {} -> {}", good.size(), tp, st.target.fullName());
  }

  /**
   * Hand a poison record to the DLQ reporter when one is configured; the framework then honours
   * errors.tolerance. With no reporter, fail the task rather than silently drop the row -- that is
   * the errors.tolerance=none default, and losing bronze CDC data unnoticed is worse than stopping.
   */
  private void reportBad(SinkRecord record, Throwable cause) {
    if (reporter != null) {
      reporter.report(record, cause);
    } else {
      throw new ConnectException(
          "Poison record at "
              + record.topic()
              + "-"
              + record.kafkaPartition()
              + "@"
              + record.kafkaOffset()
              + " and no errant-record reporter is configured (set errors.tolerance=all + a DLQ)",
          cause);
    }
  }

  /** Advance the committed offset past the flushed buffer and clear per-partition bookkeeping. */
  private void commitFlushed(TopicPartition tp, long lastOffset) {
    committed.put(tp, lastOffset + 1);
    buffers.get(tp).clear();
    bufferStartMs.remove(tp);
    bufferBytes.remove(tp);
  }

  /** Resolve (and cache) the write state for a topic's target table. */
  private TableState stateFor(String topic) {
    TableState cached = tableStates.get(topic);
    if (cached != null) {
      // catalog-managed state caches a vended SAS; re-resolve before its ~1h TTL so we re-vend
      // creds and rebuild engine/committer/snapshot rather than commit with an expiring token (H5).
      boolean stale =
          cached.committer != null && clock.getAsLong() - cached.resolvedAtMs >= REFRESH_MS;
      if (!stale) {
        return cached;
      }
      tableStates.remove(topic);
    }
    TableState st = stateBuilder.apply(topic);
    tableStates.put(topic, st);
    return st;
  }

  /** Resolve a topic to a fresh write state (no cache); the live default for {@code stateBuilder}. */
  private TableState buildState(String topic) {
    TableTarget target = resolver.resolve(topic);
    Engine engine = engineProvider.engineFor(target);
    if (target.tableId() != null) {
      Configuration conf = new Configuration();
      target.hadoopConfig().forEach(conf::set);
      UnityCatalogCommitter committer =
          new UnityCatalogCommitter(
              config.workspaceUrl(),
              credential,
              target.tableId(),
              target.tablePath(),
              conf);
      UnityCatalogCommitter.CatalogState cs = committer.catalogState();
      Snapshot snapshot =
          writer.loadCatalogSnapshot(engine, target.tablePath(), committer, cs.commits, cs.maxVersion);
      return new TableState(target, engine, committer, snapshot, clock.getAsLong());
    }
    return new TableState(target, engine, null, null, clock.getAsLong());
  }

  @Override
  public void stop() {
    synchronized (lock) {
      if (flushScheduler != null) {
        flushScheduler.shutdownNow();
      }
      if (maintenance != null) {
        maintenance.shutdown();
        try {
          maintenance.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      buffers.clear();
      bufferStartMs.clear();
      bufferBytes.clear();
      tableStates.clear();
    }
  }
}
