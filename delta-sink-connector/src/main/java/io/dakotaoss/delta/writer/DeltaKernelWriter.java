package io.dakotaoss.delta.writer;

import io.dakotaoss.delta.data.Iters;
import io.dakotaoss.delta.util.Redact;
import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.TableManager;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.commit.Committer;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.dakotaoss.delta.uc.UnityCatalogCommitter;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.transaction.UpdateTableTransactionBuilder;
import io.delta.kernel.exceptions.ConcurrentTransactionException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Delta Kernel write protocol behind a single {@code append} call.
 *
 * <p>Flow: open table (create with schema if absent); start a transaction, optionally stamped with a
 * {@code SetTransaction} idempotency id; transform logical to physical, write Parquet, generate
 * AddFile actions; commit.
 *
 * <p>Idempotency: with an {@code appId} the transaction carries {@code SetTransaction(appId, version)}.
 * On replay (same {@code appId}, {@code version} not newer than last committed) Kernel throws
 * {@link ConcurrentTransactionException}; treat as already-applied and return the existing version.
 * Lets a Connect task re-deliver a batch after a crash without duplicating data.
 *
 * <p>Catalog-managed (Unity Catalog) tables route the commit through the catalog commit coordinator
 * from the {@link Engine}; coordination happens inside {@code commit(...)}. See {@code
 * EngineProvider} (with UcTableResolver / VendedSasTokenProvider) for where UC commit/credentials are wired in.
 */
// Non-final: it is a constructor-injected collaborator of DeltaSinkTask, so tests can substitute a fake.
public class DeltaKernelWriter {

  private static final Logger LOG = LoggerFactory.getLogger(DeltaKernelWriter.class);
  private static final String ENGINE_INFO = "dakotaoss-delta-uc/0.1";

  /** Committed table version and whether the batch was newly applied. */
  public static final class Result {
    public final long version;
    public final boolean applied;

    Result(long version, boolean applied) {
      this.version = version;
      this.applied = applied;
    }
  }

  /**
   * Append one batch to the table at {@code tablePath}, creating it from {@code schema} if absent.
   * {@code partitionColumns} may be empty. Non-null {@code appId} makes the write idempotent on
   * {@code (appId, version)}. {@code tableName} is a non-secret label (the UC {@code
   * catalog.schema.table}) used only in logs/errors so the physical {@code abfss://} path stays out.
   *
   * <p>Single unpartitioned commit only. Partitioned tables need the batch grouped by partition value
   * with {@link Transaction#getWriteContext} called per partition. Not implemented.
   */
  public Result append(
      Engine engine,
      String tablePath,
      String tableName,
      StructType schema,
      List<String> partitionColumns,
      String appId,
      long version,
      FilteredColumnarBatch logicalBatch) {

    Table table = Table.forPath(engine, tablePath);

    boolean tableExists;
    try {
      table.getLatestSnapshot(engine);
      tableExists = true;
    } catch (TableNotFoundException e) {
      tableExists = false;
    }

    Operation op = tableExists ? Operation.WRITE : Operation.CREATE_TABLE;
    TransactionBuilder builder = table.createTransactionBuilder(engine, ENGINE_INFO, op);
    if (!tableExists) {
      builder = builder.withSchema(engine, schema);
      if (partitionColumns != null && !partitionColumns.isEmpty()) {
        builder = builder.withPartitionColumns(engine, partitionColumns);
      }
    }
    if (appId != null) {
      builder = builder.withTransactionId(engine, appId, version);
    }

    final Transaction txn;
    try {
      txn = builder.build(engine);
    } catch (ConcurrentTransactionException dup) {
      LOG.info("Batch ({}, {}) already committed; skipping (idempotent).", appId, version);
      return new Result(table.getLatestSnapshot(engine).getVersion(), false);
    }

    Row txnState = txn.getTransactionState(engine);
    Map<String, Literal> partitionValues = Collections.emptyMap();

    CloseableIterator<FilteredColumnarBatch> physicalData =
        Transaction.transformLogicalData(
            engine, txnState, Iters.singleton(logicalBatch), partitionValues);

    DataWriteContext writeContext = Transaction.getWriteContext(engine, txnState, partitionValues);

    // Drain+close the writtenFiles iterator (writeParquet does both); a raw iterator would leak if a
    // later ConcurrentTransactionException short-circuited the commit before it was consumed.
    List<DataFileStatus> writtenFiles = writeParquet(engine, writeContext, physicalData, tableName);

    CloseableIterator<Row> appendActions =
        Transaction.generateAppendActions(
            engine, txnState, Iters.closeable(writtenFiles.iterator()), writeContext);

    try {
      TransactionCommitResult result =
          txn.commit(engine, CloseableIterable.inMemoryIterable(appendActions));
      LOG.info("Committed {} to {} at version {}", appId, tableName, result.getVersion());
      return new Result(result.getVersion(), true);
    } catch (ConcurrentTransactionException dup) {
      LOG.info("Batch ({}, {}) already committed at commit time; skipping.", appId, version);
      return new Result(table.getLatestSnapshot(engine).getVersion(), false);
    }
  }

  /**
   * Append one batch to a catalog-managed (Unity Catalog) table, coordinating the commit through
   * {@code committer}. Uses the {@link TableManager} entry point so the committer can be attached to
   * the snapshot; Kernel's default filesystem committer refuses catalog-managed tables.
   *
   * <p>Table must already exist (created in Databricks); this is the append/update path. Write flow
   * matches {@link #append}; only snapshot/transaction construction and the committer differ.
   * {@code tableName} is the non-secret UC name used in logs/errors instead of the {@code abfss://}
   * path.
   */
  public Result appendCatalogManaged(
      Engine engine,
      String tablePath,
      String tableName,
      String appId,
      long version,
      FilteredColumnarBatch logicalBatch,
      Committer committer,
      List<ParsedLogData> catalogCommits,
      long maxCatalogVersion) {

    // Ratified commits are staged, not yet in the numbered _delta_log, so they're supplied as log
    // data. maxCatalogVersion is the newest version the catalog has ratified.
    Snapshot snapshot =
        TableManager.loadSnapshot(tablePath)
            .withCommitter(committer)
            .withLogData(catalogCommits)
            .withMaxCatalogVersion(maxCatalogVersion)
            .build(engine);

    UpdateTableTransactionBuilder builder =
        snapshot.buildUpdateTableTransaction(ENGINE_INFO, Operation.WRITE);
    if (appId != null) {
      builder = builder.withTransactionId(appId, version);
    }
    Transaction txn = builder.build(engine);

    Row txnState = txn.getTransactionState(engine);
    Map<String, Literal> partitionValues = Collections.emptyMap();

    CloseableIterator<FilteredColumnarBatch> physicalData =
        Transaction.transformLogicalData(
            engine, txnState, Iters.singleton(logicalBatch), partitionValues);
    DataWriteContext writeContext = Transaction.getWriteContext(engine, txnState, partitionValues);

    List<DataFileStatus> writtenFiles = writeParquet(engine, writeContext, physicalData, tableName);
    recordMetrics(committer, logicalBatch, writtenFiles);

    CloseableIterator<Row> appendActions =
        Transaction.generateAppendActions(
            engine, txnState, Iters.closeable(writtenFiles.iterator()), writeContext);

    TransactionCommitResult result =
        txn.commit(engine, CloseableIterable.inMemoryIterable(appendActions));
    // Backfill the ratified commit to the published _delta_log and run post-commit hooks
    // (checkpoint, checksum). Inline here (vs the streaming path's async maintain) so the all-in-one
    // call leaves a compact published log. maintain does publish + hooks exactly once: invoking them
    // here AND calling maintain would double-checkpoint and the second write would collide at a
    // checkpoint-interval version.
    maintain(engine, result);
    LOG.info(
        "Committed (catalog-managed) {} to {} at version {}",
        appId,
        tableName,
        result.getVersion());
    return new Result(result.getVersion(), true);
  }

  // ---- Streaming/low-latency path -------------------------------------------------------------
  // appendCatalogManaged re-reads the snapshot from the log and publishes + checkpoints inline on
  // every commit. For sustained low latency, split it: load the snapshot once, reuse the in-memory
  // post-commit snapshot for the next append (no log re-read), run maintenance off the commit path.

  /** Load a catalog-managed snapshot with the UC committer attached. */
  public Snapshot loadCatalogSnapshot(
      Engine engine,
      String tablePath,
      Committer committer,
      List<ParsedLogData> catalogCommits,
      long maxCatalogVersion) {
    return TableManager.loadSnapshot(tablePath)
        .withCommitter(committer)
        .withLogData(catalogCommits)
        .withMaxCatalogVersion(maxCatalogVersion)
        .build(engine);
  }

  /**
   * Append one batch to an already-loaded {@code snapshot} and commit. The result's post-commit
   * snapshot can be reused for the next append, avoiding a per-commit log re-read. Does not publish
   * or checkpoint; call {@link #maintain} for that, off the latency path. Returns null if the batch
   * was already applied.
   */
  public TransactionCommitResult appendToSnapshot(
      Engine engine,
      Snapshot snapshot,
      FilteredColumnarBatch logicalBatch,
      String appId,
      long version) {
    UpdateTableTransactionBuilder builder =
        snapshot.buildUpdateTableTransaction(ENGINE_INFO, Operation.WRITE);
    if (appId != null) {
      builder = builder.withTransactionId(appId, version);
    }
    final Transaction txn;
    try {
      txn = builder.build(engine);
    } catch (ConcurrentTransactionException dup) {
      LOG.info("Batch ({}, {}) already committed; skipping (idempotent).", appId, version);
      return null;
    }
    Row txnState = txn.getTransactionState(engine);
    Map<String, Literal> partitionValues = Collections.emptyMap();
    CloseableIterator<FilteredColumnarBatch> physicalData =
        Transaction.transformLogicalData(
            engine, txnState, Iters.singleton(logicalBatch), partitionValues);
    DataWriteContext writeContext = Transaction.getWriteContext(engine, txnState, partitionValues);
    List<DataFileStatus> writtenFiles = writeParquet(engine, writeContext, physicalData, "snapshot append");
    recordMetrics(txn.getCommitter(), logicalBatch, writtenFiles);
    CloseableIterator<Row> appendActions =
        Transaction.generateAppendActions(
            engine, txnState, Iters.closeable(writtenFiles.iterator()), writeContext);
    try {
      return txn.commit(engine, CloseableIterable.inMemoryIterable(appendActions));
    } catch (ConcurrentTransactionException dup) {
      LOG.info("Batch ({}, {}) already committed at commit time; skipping.", appId, version);
      return null;
    }
  }

  /**
   * Backfill the committed version to the published log and run post-commit hooks (checkpoint,
   * checksum). Safe to run async: UC already holds the ratified commit durably, so this only compacts
   * the published log and never affects commit durability.
   */
  public void maintain(Engine engine, TransactionCommitResult result) {
    if (result.getPostCommitSnapshot().isPresent()) {
      try {
        result.getPostCommitSnapshot().get().publish(engine);
      } catch (io.delta.kernel.commit.PublishFailedException e) {
        throw new RuntimeException("Publish (backfill) failed", e);
      }
    }
    for (io.delta.kernel.hook.PostCommitHook hook : result.getPostCommitHooks()) {
      try {
        hook.threadSafeInvoke(engine);
      } catch (io.delta.kernel.exceptions.CheckpointAlreadyExistsException e) {
        // A checkpoint for this version is already present (e.g. left by an external/Databricks-side
        // write or an earlier run). The checkpoint is a published-log optimization, never commit
        // durability -- UC holds the ratified commit -- so an existing one is a no-op, not a failure.
        LOG.debug("Checkpoint already present for hook {}; skipping (idempotent).", hook.getType());
      } catch (IOException e) {
        // Redact: ABFS IOExceptions embed the request URL incl. the SAS query string.
        LOG.warn("Post-commit hook {} failed: {}", hook.getType(), Redact.message(e));
      }
    }
  }

  /** Write the batch's Parquet files, materialized to a list so file count/size metrics can be read. */
  private static List<DataFileStatus> writeParquet(
      Engine engine,
      DataWriteContext writeContext,
      CloseableIterator<FilteredColumnarBatch> physicalData,
      String what) {
    final CloseableIterator<DataFileStatus> it;
    try {
      it =
          engine
              .getParquetHandler()
              .writeParquetFiles(
                  writeContext.getTargetDirectory(),
                  physicalData,
                  writeContext.getStatisticsColumns());
    } catch (IOException e) {
      throw new RuntimeException("Failed writing Parquet for " + what, e);
    }
    List<DataFileStatus> files = new ArrayList<>();
    while (it.hasNext()) {
      files.add(it.next());
    }
    try {
      it.close();
    } catch (Exception ignore) {
      // best effort
    }
    return files;
  }

  /** Hand per-commit write metrics to the UC committer so the commit's operationMetrics is populated. */
  private static void recordMetrics(
      Committer committer, FilteredColumnarBatch batch, List<DataFileStatus> files) {
    if (committer instanceof UnityCatalogCommitter) {
      long bytes = 0;
      for (DataFileStatus f : files) {
        bytes += f.getSize();
      }
      ((UnityCatalogCommitter) committer)
          .setPendingMetrics(batch.getData().getSize(), files.size(), bytes);
    }
  }
}
