package io.dakotaoss.delta;

import io.dakotaoss.delta.auth.CredentialProvider;
import io.dakotaoss.delta.auth.Credentials;
import io.dakotaoss.delta.model.TableTarget;
import io.dakotaoss.delta.schema.RecordConverter;
import io.dakotaoss.delta.schema.SchemaEvolution;
import io.dakotaoss.delta.schema.SchemaMapper;
import io.dakotaoss.delta.uc.TableResolver;
import io.dakotaoss.delta.uc.UcColumnMapper;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
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
 * the per-partition {@code SetTransaction(appId, offset)} stamp this gives effectively-once
 * delivery across restarts.
 */
public final class DeltaSinkTask extends SinkTask {

  static final String VERSION = "0.1.0";
  private static final Logger LOG = LoggerFactory.getLogger(DeltaSinkTask.class);

  // Hard ceiling on rows buffered across all partitions; past it we apply backpressure so a
  // stalled flush (e.g. UC outage) cannot grow the heap without bound.
  private static final int MAX_BUFFERED_RECORDS = 1_000_000;
  // Re-resolve a cached catalog-managed table before its vended SAS (~1h TTL) can expire.
  private static final long REFRESH_MS = 40 * 60 * 1000;

  private DeltaSinkConfig config;
  // one provider per task, shared across all tables: a single token (refreshed on its own for the
  // oauth/entra modes) authenticates every UC call this task makes.
  private CredentialProvider credential;
  private TableResolver resolver;
  // Prod-only (null under the test constructor): the UC client + typed resolver for auto-creating
  // an
  // absent catalog-managed table on first write. The auto-create path is guarded on uc != null.
  private UnityCatalogClient uc;
  private UcTableResolver ucResolver;
  private boolean autoCreate;
  // additive schema evolution policy (catalog-managed only); NONE = today's fail-closed DLQ
  // routing.
  private SchemaEvolution.Policy evolution = SchemaEvolution.Policy.NONE;
  private EngineProvider engineProvider;
  private DeltaKernelWriter writer;
  private String connectorName;
  private int flushSize;
  private long flushBytes;
  private long flushIntervalMs;

  private final Object lock = new Object();
  // per-partition rows/bytes/start bookkeeping; all access is under lock (built in start()/test
  // ctor
  // once flush.bytes is known, so it can skip byte estimation when the dial is off).
  private RecordBuffer buffer;
  // Concurrent: with flush.concurrency>1 these are touched by per-table commit tasks running off
  // the
  // task thread. committed is written per-partition, tableStates per-table -- disjoint keys per
  // task.
  private final Map<TopicPartition, Long> committed = new ConcurrentHashMap<>();
  private final Map<String, TableState> tableStates = new ConcurrentHashMap<>();

  private ExecutorService maintenance;
  private ScheduledExecutorService flushScheduler;
  // Commit independent tables in parallel (1 = serial, today's behavior). Bounds in-flight commits;
  // one task per table so same-table commits stay serialized. Null when flushConcurrency == 1.
  private int flushConcurrency = 1;
  private ExecutorService flushExecutor;
  // Connect's DLQ hook; null when the test constructor is used or no reporter is configured.
  private ErrantRecordReporter reporter;

  // Test seams. Defaults match prod; tests inject to drive the otherwise only-in-prod backpressure,
  // ~40min TTL re-resolve, and async-maintenance failure paths without 1M rows or a live UC. The
  // class is final, so these are injection points rather than overridable methods.
  private int maxBufferedRecords = MAX_BUFFERED_RECORDS;
  private java.util.function.LongSupplier clock = System::currentTimeMillis;
  // builds a fresh (uncached) write state for a topic; the live impl resolves + opens UC, a test
  // can
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
    final long resolvedAtMs; // clock reading at resolve; used to expire vended SAS before TTL

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
    this.buffer = new RecordBuffer(flushBytes > 0);
    this.evolution = SchemaEvolution.Policy.from(config.schemaEvolution());
    this.flushConcurrency = config.flushConcurrency();
    if (flushConcurrency > 1) {
      this.flushExecutor = Executors.newFixedThreadPool(flushConcurrency);
    }
    this.maintenance = Executors.newSingleThreadExecutor();
  }

  /**
   * Test seam: supply the DLQ reporter that {@code start()} would otherwise pull from the context.
   */
  void injectReporter(ErrantRecordReporter reporter) {
    this.reporter = reporter;
  }

  /**
   * Test seam: shrink the backpressure ceiling so the cap is reachable without millions of rows.
   */
  void setMaxBufferedRecordsForTest(int cap) {
    this.maxBufferedRecords = cap;
  }

  /** Test seam: drive the SAS-TTL re-resolve clock instead of waiting ~40 min of wall time. */
  void setClockForTest(java.util.function.LongSupplier clock) {
    this.clock = clock;
  }

  /**
   * Test seam: capture the redacted text the async-maintenance failure path would otherwise log.
   */
  void setAsyncMaintenanceErrorSinkForTest(java.util.function.Consumer<String> sink) {
    this.asyncMaintenanceErrorSink = sink;
  }

  /** Test seam: supply a write-state builder so a catalog-managed flush needs no live UC. */
  void setStateBuilderForTest(java.util.function.Function<String, TableState> builder) {
    this.stateBuilder = builder;
  }

  /**
   * Test seam: clock reading used to stamp a {@link TableState} built outside the live resolver.
   */
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
    this.buffer = new RecordBuffer(flushBytes > 0);
    // One credential provider for the whole task: for oauth/entra it mints and refreshes tokens on
    // its own (before expiry); for pat it reads the config token per request. Either way the UC
    // clients just see a Supplier<String> that always yields a valid token.
    this.credential = Credentials.fromConfig(config);
    this.uc = new UnityCatalogClient(config.workspaceUrl(), credential);
    this.ucResolver =
        new UcTableResolver(
            uc, config.tableNameFormat(), config.topicToTable(), config.partitionColumns());
    this.resolver = ucResolver;
    this.autoCreate = config.autoCreateTables();
    this.evolution = SchemaEvolution.Policy.from(config.schemaEvolution());
    this.engineProvider = EngineProvider.hadoop();
    this.writer = new DeltaKernelWriter();
    this.maintenance = Executors.newSingleThreadExecutor();
    this.flushConcurrency = config.flushConcurrency();
    if (flushConcurrency > 1) {
      this.flushExecutor = Executors.newFixedThreadPool(flushConcurrency);
    }
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
      // pause and re-deliver this same batch once flushes drain the buffers.
      int buffered = buffer.totalRows();
      if (buffered + records.size() > maxBufferedRecords) {
        throw new RetriableException(
            "buffered records "
                + buffered
                + " + "
                + records.size()
                + " would exceed cap "
                + maxBufferedRecords
                + "; applying backpressure");
      }
      long now = clock.getAsLong();
      for (SinkRecord record : records) {
        buffer.add(record, now);
      }
      // early flush: size/bytes thresholds commit ahead of the interval tick
      for (TopicPartition tp : buffer.partitions()) {
        if (buffer.tripped(tp, flushSize, flushBytes)) {
          flush(tp);
        }
      }
    }
  }

  /** Scheduled tick: flush any non-empty buffer so queued rows commit within flush.interval.ms. */
  private void flushOnInterval() {
    synchronized (lock) {
      try {
        flushDirty();
      } catch (Exception e) {
        // redact: the cause can embed a vended SAS
        LOG.error("Scheduled flush failed: {}", Redact.message(e));
      }
    }
  }

  /**
   * Flush every dirty partition. Serially (flush.concurrency==1, today's behavior) or, when >1 and
   * more than one table is dirty, with one commit task per table on the bounded {@code
   * flushExecutor} -- different tables commit in parallel (overlapping WAN round-trips) while a
   * table's own partitions stay serial inside its task (so its reused snapshot is touched by one
   * thread, preserving order). Caller holds {@code lock} for the whole call incl. the await, so no
   * other flush runs meanwhile and the only concurrent state is the per-table maps
   * (committed/tableStates) and the synchronized buffer.
   */
  private void flushDirty() {
    List<TopicPartition> parts = buffer.partitions();
    Map<String, List<TopicPartition>> byTable = new LinkedHashMap<>();
    for (TopicPartition tp : parts) {
      byTable.computeIfAbsent(tp.topic(), k -> new ArrayList<>()).add(tp);
    }
    if (flushExecutor == null || byTable.size() <= 1) {
      for (TopicPartition tp : parts) {
        flush(tp);
      }
      return;
    }
    List<Future<?>> futures = new ArrayList<>();
    for (List<TopicPartition> group : byTable.values()) {
      futures.add(
          flushExecutor.submit(
              () -> {
                for (TopicPartition tp : group) {
                  flush(tp); // same-table partitions serial -> snapshot touched by one thread
                }
                return null;
              }));
    }
    RuntimeException failure = null;
    for (Future<?> f : futures) {
      try {
        f.get();
      } catch (ExecutionException ee) {
        // one table's commit failed (already DLQ-handled if a reporter was set, else fatal). Let
        // the
        // other tables finish, then surface the first failure to fail the task -- its offset never
        // advanced, so Connect redelivers it; tables that succeeded keep their advanced offsets.
        RuntimeException re =
            ee.getCause() instanceof RuntimeException
                ? (RuntimeException) ee.getCause()
                : new ConnectException(Redact.message(ee.getCause()));
        if (failure == null) {
          failure = re;
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new ConnectException("flush interrupted", ie);
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    synchronized (lock) {
      flushDirty();
      Map<TopicPartition, OffsetAndMetadata> safe = new HashMap<>();
      for (Map.Entry<TopicPartition, Long> e : committed.entrySet()) {
        safe.put(e.getKey(), new OffsetAndMetadata(e.getValue()));
      }
      return safe;
    }
  }

  /**
   * Commit one topic-partition's buffer as a single Delta transaction. Caller holds {@code lock}.
   */
  private void flush(TopicPartition tp) {
    List<SinkRecord> batch = buffer.rows(tp);
    if (batch == null || batch.isEmpty()) {
      return;
    }
    long lastOffset = batch.get(batch.size() - 1).kafkaOffset();

    // Split into writable rows (Struct values on a single reference schema) and poison. reportBad
    // routes poison to the DLQ (the commit then advances the offset past it), or fails the task
    // when
    // no DLQ reporter is configured -- the offset never advances over poison silently.
    PoisonPartitioner split = PoisonPartitioner.of(batch);
    for (SinkRecord bad : split.poison) {
      reportBad(
          bad, new ConnectException("poison record: non-Struct/null value or schema mismatch"));
    }
    Schema refSchema = split.refSchema;
    List<SinkRecord> good = split.good;

    if (good.isEmpty()) {
      // everything was skipped; still advance/clear so the partition makes progress.
      commitFlushed(tp, lastOffset);
      LOG.warn("Flush for {} dropped all {} records as poison", tp, batch.size());
      return;
    }

    String appId = connectorName + ":" + tp.topic() + "-" + tp.partition();
    // Create the catalog-managed table on first write if it's absent (auto.create.tables). Only on
    // a
    // cache miss, so steady-state flushes don't pay an extra getTable. refSchema gives the columns.
    if (autoCreate && uc != null && !tableStates.containsKey(tp.topic())) {
      ensureCatalogTable(tp.topic(), refSchema);
    }
    TableState st = stateFor(tp.topic());
    try {
      StructType kernelSchema = SchemaMapper.toKernel(refSchema);
      // Additive schema evolution (catalog-managed only): if the incoming batch adds nullable
      // columns,
      // ALTER the table Databricks-side, then reload so the snapshot reflects the new
      // schema/version
      // before the append. A non-additive change is poison -> DLQ (or fail the task). The append's
      // SetTransaction(appId, offset) makes a replay a no-op, and the column-present diff makes the
      // re-evolve a no-op, so a replayed batch neither double-appends nor re-evolves.
      if (st.committer != null && evolution == SchemaEvolution.Policy.ADD && uc != null) {
        SchemaEvolution.Result ev = SchemaEvolution.diff(st.snapshot.getSchema(), kernelSchema);
        if (ev.breaking) {
          for (SinkRecord record : good) {
            reportBad(record, new ConnectException("non-additive schema change; routed to DLQ"));
          }
          commitFlushed(tp, lastOffset);
          LOG.warn(
              "Flush for {} routed {} rows to DLQ (non-additive schema change)", tp, good.size());
          return;
        }
        if (ev.changed()) {
          alterAddColumns(st.target.fullName(), refSchema, ev.addedColumns);
          tableStates.remove(tp.topic());
          st = stateFor(tp.topic());
        }
      }
      FilteredColumnarBatch data = RecordConverter.toBatch(kernelSchema, refSchema, good);
      if (st.committer != null) {
        // Catalog-managed: reuse the in-memory snapshot, run backfill/checkpoint async. Invariant:
        // st.snapshot is read and advanced only here, on the flush/commit thread under lock; the
        // async
        // maintenance task captures the immutable commit result + engine and never touches
        // st.snapshot.
        TransactionCommitResult result =
            writer.appendToSnapshot(st.engine, st.snapshot, data, appId, lastOffset);
        if (result != null) {
          st.snapshot = result.getPostCommitSnapshot().orElse(st.snapshot);
          final Engine eng = st.engine;
          final TransactionCommitResult committed = result;
          final String topic = tp.topic();
          maintenance.submit(() -> runMaintenance(eng, committed, topic));
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
      // Redact before the failure leaves our code and drop the raw cause: ABFS IOExceptions embed
      // the
      // request URL incl. the vended SAS, and whatever we hand off persists -- DLQ record headers,
      // and
      // Connect's task-failure log (which prints the whole cause chain). Carry only the redacted
      // text.
      ConnectException safe =
          new ConnectException("flush failed for " + tp + ": " + Redact.message(e));
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
   * Run async backfill/checkpoint off the commit path. UC already holds the ratified commit, so a
   * failed publish/checkpoint is non-fatal -- swallow it (redacted; the cause can embed a vended
   * SAS) rather than fail the producer's commit. Package-private so a fault-injection test can
   * drive it.
   */
  void runMaintenance(Engine engine, TransactionCommitResult result, String topic) {
    try {
      writer.maintain(engine, result);
    } catch (Exception e) {
      asyncMaintenanceErrorSink.accept(topic + ": " + Redact.message(e));
    }
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
    buffer.clear(tp);
  }

  /** Resolve (and cache) the write state for a topic's target table. */
  private TableState stateFor(String topic) {
    TableState cached = tableStates.get(topic);
    if (cached != null) {
      // catalog-managed state caches a vended SAS; re-resolve before its ~1h TTL so we re-vend
      // creds and rebuild engine/committer/snapshot rather than commit with an expiring token.
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

  /**
   * Resolve a topic to a fresh write state (no cache); the live default for {@code stateBuilder}.
   */
  private TableState buildState(String topic) {
    TableTarget target = resolver.resolve(topic);
    Engine engine = engineProvider.engineFor(target);
    if (target.tableId() != null) {
      Configuration conf = new Configuration();
      target.hadoopConfig().forEach(conf::set);
      UnityCatalogCommitter committer =
          new UnityCatalogCommitter(
              config.workspaceUrl(), credential, target.tableId(), target.tablePath(), conf);
      UnityCatalogCommitter.CatalogState cs = committer.catalogState();
      Snapshot snapshot =
          writer.loadCatalogSnapshot(
              engine, target.tablePath(), committer, cs.commits, cs.maxVersion);
      return new TableState(target, engine, committer, snapshot, clock.getAsLong());
    }
    return new TableState(target, engine, null, null, clock.getAsLong());
  }

  /**
   * Create the topic's catalog-managed table if absent (auto.create.tables): register it in UC with
   * a schema derived from {@code valueSchema} (columns nullable), then write its v0 schema commit
   * so the normal append path takes over. v0 carries no data; the first batch is appended as v1
   * with its SetTransaction stamp, so a crash between create and offset-commit can't duplicate it.
   * No-op if the table exists; a non-404 error is left for stateFor's resolve to surface
   * (redacted).
   */
  private void ensureCatalogTable(String topic, org.apache.kafka.connect.data.Schema valueSchema) {
    String fullName = ucResolver.nameFor(topic);
    try {
      uc.getTable(fullName);
      return; // already exists
    } catch (UnityCatalogClient.NotFoundException absent) {
      // fall through to create
    } catch (Exception e) {
      return; // auth/network/etc.: let stateFor's resolve raise it with redaction
    }
    String warehouse = config.warehouseId();
    if (warehouse == null || warehouse.isEmpty()) {
      throw new ConnectException(
          "auto-create needs " + DeltaSinkConfig.WAREHOUSE_ID + " set, or pre-create " + fullName);
    }
    try {
      // DDL via a SQL warehouse: Databricks writes the table's v0 (an external engine cannot commit
      // v0 -- UC's create protocol differs from its commit path), then the normal append writes v1.
      String ddl =
          "CREATE TABLE IF NOT EXISTS "
              + fullName
              + " ("
              + UcColumnMapper.ddlColumnDefs(valueSchema)
              + ") TBLPROPERTIES ('delta.feature.catalogManaged'='supported')";
      uc.executeStatement(warehouse, ddl);
      LOG.info("Auto-created catalog-managed table {}", fullName);
    } catch (Exception e) {
      throw new ConnectException("auto-create failed for " + fullName + ": " + Redact.message(e));
    }
  }

  /**
   * Add the (nullable) {@code columns} to a catalog-managed table via {@code ALTER TABLE ... ADD
   * COLUMNS} on the SQL warehouse (schema.evolution). Databricks commits the DDL; the caller then
   * reloads the snapshot and the normal append writes the wider rows -- no column mapping needed
   * (the Kernel write path cannot touch a column-mapping table on 4.2.0). A concurrent evolver may
   * have already added the columns; that "already exists" is benign (we reload and continue either
   * way).
   */
  private void alterAddColumns(
      String fullName, org.apache.kafka.connect.data.Schema valueSchema, List<String> columns) {
    String ddl =
        "ALTER TABLE "
            + fullName
            + " ADD COLUMNS ("
            + UcColumnMapper.addColumnsDdl(valueSchema, columns)
            + ")";
    try {
      uc.executeStatement(config.warehouseId(), ddl);
      LOG.info("Evolved {} (+columns {})", fullName, columns);
    } catch (Exception e) {
      if (alreadyExists(e)) {
        LOG.info(
            "Columns {} already present on {} (concurrent evolve); continuing", columns, fullName);
        return;
      }
      throw new ConnectException("schema evolve failed for " + fullName + ": " + Redact.message(e));
    }
  }

  /**
   * A concurrent ALTER from another task can win the race; Databricks then rejects ours as a dup.
   */
  private static boolean alreadyExists(Throwable t) {
    for (Throwable c = t; c != null && c != c.getCause(); c = c.getCause()) {
      String m = c.getMessage();
      if (m != null) {
        String s = m.toLowerCase(java.util.Locale.ROOT);
        if (s.contains("already exists") || s.contains("fields_already_exist")) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public void stop() {
    ScheduledExecutorService sched;
    ExecutorService maint;
    ExecutorService flushEx;
    synchronized (lock) {
      sched = flushScheduler;
      maint = maintenance;
      flushEx = flushExecutor;
      flushScheduler = null;
      maintenance = null;
      flushExecutor = null;
      if (buffer != null) {
        buffer.clearAll();
      }
      tableStates.clear();
    }
    // Drain the executors OUTSIDE the lock: awaitTermination can block up to 30s while async
    // maintenance finishes, and put()/the interval flush also take the lock -- holding it here
    // would
    // freeze the poller and risk a Connect stop-timeout. flush() and stop() still serialize on the
    // lock, so no flush runs concurrently with the field nulling above.
    if (sched != null) {
      sched.shutdownNow();
    }
    if (flushEx != null) {
      // a flushDirty in flight holds the lock, so by here no commit task is running; just release
      // it.
      flushEx.shutdownNow();
    }
    if (maint != null) {
      maint.shutdown();
      try {
        maint.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
