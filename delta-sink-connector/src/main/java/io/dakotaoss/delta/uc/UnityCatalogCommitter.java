package io.dakotaoss.delta.uc;

import io.delta.kernel.commit.CatalogCommitter;
import io.delta.kernel.commit.CommitFailedException;
import io.delta.kernel.commit.CommitMetadata;
import io.delta.kernel.commit.CommitResponse;
import io.delta.kernel.commit.PublishFailedException;
import io.delta.kernel.commit.PublishMetadata;
import io.dakotaoss.delta.data.Iters;
import io.dakotaoss.delta.util.Redact;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.files.ParsedCatalogCommitData;
import io.delta.kernel.internal.files.ParsedDeltaData;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.storage.commit.Commit;
import io.delta.storage.commit.GetCommitsResponse;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delta Kernel {@link Committer} that coordinates commits through Unity Catalog so an external engine
 * can write to a <b>catalog-managed</b> Delta table.
 *
 * <p>Kernel's default committer only writes the filesystem {@code _delta_log} and refuses
 * catalog-managed tables. For a catalog-managed table the commit must be ratified by UC: the writer
 * stages the commit file under {@code _delta_log/_staged_commits/} and asks UC to register it at the
 * target version (first-writer-wins; losers get a conflict to retry).
 *
 * <p>The UC Delta commits REST API is delegated to delta-storage's {@link UCTokenBasedRestClient},
 * which wraps the Unity Catalog client SDK. This class adapts that onto Kernel's {@link Committer}
 * contract.
 */
public final class UnityCatalogCommitter implements CatalogCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(UnityCatalogCommitter.class);

  private final UCClient ucClient;
  private final String tableId;
  private final URI tableUri;
  private final Configuration hadoopConf;
  // Highest version this committer has backfilled to the published log; told to UC on the next commit
  // so getCommits stops returning already-published commits.
  private long highestPublishedVersion = -1;
  // Per-commit metrics supplied by the writer immediately before commit; consumed once so the
  // emitted commitInfo carries operationMetrics. -1 = not supplied (pass actions through unchanged).
  private long pendingNumRows = -1;
  private long pendingNumFiles = -1;
  private long pendingNumBytes = -1;

  /**
   * @param workspaceUrl UC REST base, e.g. https://adb-xxxx.azuredatabricks.net
   * @param token bearer token supplier (PAT or AAD access token) for a principal with EXTERNAL USE
   *     SCHEMA; read per request so a re-minted token is picked up and no extra durable copy is held
   * @param tableId UC table id (the {@code table_id} from GET .../tables)
   * @param tableStorageLocation the table's {@code abfss://} storage location
   * @param hadoopConf Hadoop config carrying the vended ABFS SAS (used to stat the staged commit file)
   */
  public UnityCatalogCommitter(
      String workspaceUrl,
      Supplier<String> token,
      String tableId,
      String tableStorageLocation,
      Configuration hadoopConf) {
    this.tableId = tableId;
    this.tableUri = URI.create(tableStorageLocation);
    this.hadoopConf = hadoopConf;
    this.ucClient =
        new UCTokenBasedRestClient(
            workspaceUrl, new BearerTokenProvider(token), Collections.emptyMap());
  }

  /** Convenience for tests / fixed tokens; production passes a {@link Supplier} reading the config. */
  public UnityCatalogCommitter(
      String workspaceUrl,
      String token,
      String tableId,
      String tableStorageLocation,
      Configuration hadoopConf) {
    this(workspaceUrl, () -> token, tableId, tableStorageLocation, hadoopConf);
  }

  /** What UC knows about a table's log: the ratified (staged, not-yet-published) commits + latest version. */
  public static final class CatalogState {
    public final List<ParsedLogData> commits;
    public final long maxVersion;

    CatalogState(List<ParsedLogData> commits, long maxVersion) {
      this.commits = commits;
      this.maxVersion = maxVersion;
    }
  }

  /**
   * Fetch the catalog's view of the table in one call: the ratified commit files (which the snapshot
   * read needs via {@code SnapshotBuilder.withLogData}, since they are staged and not yet published to
   * the numbered {@code _delta_log}) and the latest table version ({@code withMaxCatalogVersion}).
   */
  public CatalogState catalogState() {
    try {
      GetCommitsResponse resp =
          ucClient.getCommits(tableId, tableUri, Optional.empty(), Optional.empty());
      FileSystem fs = FileSystem.get(tableUri, hadoopConf);
      List<ParsedLogData> commits = new ArrayList<>();
      for (Commit c : resp.getCommits()) {
        // Skip commits already backfilled to the published log: Kernel reads those directly, and
        // passing them as log data too would double-count the version.
        if (fs.exists(new Path(logDir() + "/" + publishedName(c.getVersion())))) {
          continue;
        }
        FileStatus h = c.getFileStatus();
        io.delta.kernel.utils.FileStatus ks =
            io.delta.kernel.utils.FileStatus.of(
                h.getPath().toString(), h.getLen(), h.getModificationTime());
        commits.add(ParsedCatalogCommitData.forFileStatus(ks));
      }
      // UC returns commits newest-first; Kernel's withLogData requires ascending, contiguous order.
      commits.sort(java.util.Comparator.comparingLong(ParsedLogData::getVersion));
      return new CatalogState(commits, resp.getLatestTableVersion());
    } catch (Exception e) {
      // Redact and drop the raw cause: UC/ABFS error text can carry a vended SAS or bearer token, and
      // this propagates from stateFor() (outside flush's redacting catch) to Connect's task-failure log.
      throw new RuntimeException("UC getCommits failed for " + tableId + ": " + Redact.message(e));
    }
  }

  /**
   * Backfill ratified commits to the published {@code _delta_log} so subsequent reads don't have to
   * replay an unbounded set of staged catalog commits. Kernel calls this via {@code Snapshot.publish}
   * after a commit; copies each staged commit file to its numbered {@code NN...N.json}.
   */
  @Override
  public void publish(Engine engine, PublishMetadata publishMetadata) {
    String logPath = publishMetadata.getLogPath();
    try {
      FileSystem fs = FileSystem.get(URI.create(logPath), hadoopConf);
      for (ParsedCatalogCommitData c : publishMetadata.getAscendingCatalogCommits()) {
        Path target = new Path(logPath + "/" + publishedName(c.getVersion()));
        if (fs.exists(target)) {
          continue;
        }
        try (org.apache.hadoop.fs.FSDataInputStream in =
                fs.open(new Path(c.getFileStatus().getPath()));
            org.apache.hadoop.fs.FSDataOutputStream out = fs.create(target, false)) {
          org.apache.hadoop.io.IOUtils.copyBytes(in, out, hadoopConf, false);
        }
        highestPublishedVersion = Math.max(highestPublishedVersion, c.getVersion());
      }
    } catch (Exception e) {
      throw new PublishFailedException("UC publish (backfill) failed for " + tableId, e);
    }
  }

  private String logDir() {
    String base = tableUri.toString();
    return base.endsWith("/") ? base + "_delta_log" : base + "/_delta_log";
  }

  private static String publishedName(long version) {
    return String.format("%020d.json", version);
  }

  @Override
  public CommitResponse commit(
      Engine engine, CloseableIterator<Row> finalizedActions, CommitMetadata commitMetadata)
      throws CommitFailedException {

    long version = commitMetadata.getVersion();
    String stagedPath = commitMetadata.generateNewStagedCommitFilePath();
    try {
      // 1. Stage: write the finalized actions (enriched commit info) under _staged_commits/.
      engine.getJsonHandler().writeJsonFileAtomically(stagedPath, enrich(finalizedActions, commitMetadata), false);

      // 2. Stat the staged file - UC's commit payload needs its size + timestamp.
      FileSystem fs = FileSystem.get(URI.create(stagedPath), hadoopConf);
      FileStatus staged = fs.getFileStatus(new Path(stagedPath));
      long commitTs = staged.getModificationTime();

      // 3. Ask UC to ratify at the target version (first-writer-wins).
      Commit commit = new Commit(version, staged, commitTs);
      Optional<Long> backfilled =
          highestPublishedVersion >= 0 ? Optional.of(highestPublishedVersion) : Optional.<Long>empty();
      ucClient.commit(
          tableId,
          tableUri,
          Optional.of(commit),
          backfilled, // lastKnownBackfilledVersion: lets UC prune already-published commits
          false, // disownCommit
          Optional.empty(), // metadata (append: unchanged)
          Optional.empty(), // protocol (append: unchanged)
          Optional.empty()); // uniform (Iceberg)

      // Log the UC table id (a UUID), never stagedPath: it's an abfss:// location disclosing layout.
      LOG.info("UC ratified commit v{} for table {}", version, tableId);

      io.delta.kernel.utils.FileStatus kernelStatus =
          io.delta.kernel.utils.FileStatus.of(stagedPath, staged.getLen(), commitTs);
      return new CommitResponse(ParsedDeltaData.forFileStatus(kernelStatus));

    } catch (io.delta.storage.commit.CommitFailedException e) {
      // Redact: UC/ABFS error text can carry a vended SAS or bearer token.
      throw new CommitFailedException(e.getRetryable(), e.getConflict(), Redact.text(e.getMessage()), e);
    } catch (Exception e) {
      throw new CommitFailedException(
          false, false, "UC catalog commit failed: " + Redact.message(e), e);
    }
  }

  /**
   * Per-commit write metrics, set by the writer immediately before {@code commit} and consumed once.
   * Lets the emitted {@code commitInfo} carry {@code operationMetrics} (Kernel leaves them empty).
   */
  public void setPendingMetrics(long numRows, long numFiles, long numBytes) {
    this.pendingNumRows = numRows;
    this.pendingNumFiles = numFiles;
    this.pendingNumBytes = numBytes;
  }

  /**
   * Replace Kernel's bare commit-info action with one that carries {@code operationParameters},
   * {@code operationMetrics}, and {@code isBlindAppend}. Kernel's low-level writer leaves these empty;
   * Spark-written tables populate them, so match that for a consistent Delta history. Pass-through
   * when the writer supplied no metrics.
   */
  private CloseableIterator<Row> enrich(CloseableIterator<Row> actions, CommitMetadata commitMetadata) {
    if (pendingNumRows < 0) {
      return actions;
    }
    List<Row> out = new ArrayList<>();
    try {
      while (actions.hasNext()) {
        Row row = actions.next();
        if (!row.isNullAt(SingleAction.COMMIT_INFO_ORDINAL)) {
          out.add(SingleAction.createCommitInfoSingleAction(enrichedCommitInfo(commitMetadata).toRow()));
        } else {
          out.add(row);
        }
      }
    } finally {
      try {
        actions.close();
      } catch (Exception ignore) {
        // best effort
      }
    }
    pendingNumRows = pendingNumFiles = pendingNumBytes = -1;
    return Iters.closeable(out.iterator());
  }

  private CommitInfo enrichedCommitInfo(CommitMetadata commitMetadata) {
    CommitInfo base = commitMetadata.getCommitInfo();
    Map<String, String> params = new HashMap<>(base.getOperationParameters());
    params.putIfAbsent("mode", "Append");
    Map<String, String> metrics = new HashMap<>(base.getOperationMetrics());
    metrics.put("numOutputRows", Long.toString(pendingNumRows));
    metrics.put("numFiles", Long.toString(pendingNumFiles));
    metrics.put("numOutputBytes", Long.toString(pendingNumBytes));
    return new CommitInfo(
        base.getInCommitTimestamp(),
        base.getTimestamp(),
        base.getEngineInfo(),
        base.getOperation(),
        params,
        Optional.of(Boolean.TRUE), // an append is a blind append
        base.getTxnId(),
        metrics);
  }
}
