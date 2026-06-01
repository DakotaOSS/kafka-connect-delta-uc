package io.dakotaoss.delta.uc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.dakotaoss.delta.data.Iters;
import io.delta.kernel.commit.CommitMetadata;
import io.delta.kernel.commit.PublishMetadata;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.Format;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.files.ParsedCatalogCommitData;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.storage.commit.Commit;
import io.delta.storage.commit.GetCommitsResponse;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Offline behavior tests for the parts of {@link UnityCatalogCommitter} that the env-gated Live tests
 * never assert: commitInfo enrichment (consume-once metrics), and the catalogState sort/dedup +
 * publish backfill paths driven against a fake {@link UCClient} and a local {@code _delta_log}. These
 * exercise Kernel internals (CommitInfo/ParsedCatalogCommitData/PublishMetadata) so a regression on a
 * {@code delta.kernel.version} bump fails here rather than only in the skipped Live suite.
 */
class UnityCatalogCommitterTest {

  // ---- enrich / enrichedCommitInfo -------------------------------------------------------------

  @Test
  void enrichedCommitInfoStampsMetricsModeAndBlindAppend() throws Exception {
    UnityCatalogCommitter committer = committer("file:/tmp/unused");
    committer.setPendingMetrics(42, 3, 4096);

    CommitInfo enriched = committer.enrichedCommitInfo(commitMetadata(operationParams()));

    assertEquals("42", enriched.getOperationMetrics().get("numOutputRows"));
    assertEquals("3", enriched.getOperationMetrics().get("numFiles"));
    assertEquals("4096", enriched.getOperationMetrics().get("numOutputBytes"));
    // Kernel leaves these empty; we stamp them so the Delta history matches a Spark-written table.
    assertEquals("Append", enriched.getOperationParameters().get("mode"));
    assertEquals(Optional.of(Boolean.TRUE), enriched.getIsBlindAppend());
    // base fields carried through unchanged
    assertEquals(Optional.of("WRITE"), enriched.getOperation());
    assertEquals(Optional.of("txn-1"), enriched.getTxnId());
  }

  @Test
  void enrichSwapsCommitInfoRowAndPassesOthersThrough() throws Exception {
    UnityCatalogCommitter committer = committer("file:/tmp/unused");
    committer.setPendingMetrics(10, 1, 100);

    Row commitInfoRow = SingleAction.createCommitInfoSingleAction(baseCommitInfo(operationParams()).toRow());
    Row nonCommitInfoRow = nonCommitInfoRow();

    List<Row> out = drain(committer.enrich(actions(commitInfoRow, nonCommitInfoRow), commitMetadata(operationParams())));

    assertEquals(2, out.size());
    // the non-commit-info action is forwarded by identity; the commit-info action is replaced with a
    // fresh single-action carrying the enriched commit info (its content is asserted directly above).
    assertSame(nonCommitInfoRow, out.get(1));
    assertFalse(out.get(0).isNullAt(SingleAction.COMMIT_INFO_ORDINAL), "commit-info action must remain present");
    assertEquals("WRITE", out.get(0).getStruct(SingleAction.COMMIT_INFO_ORDINAL)
        .getString(CommitInfo.FULL_SCHEMA.indexOf("operation")));
  }

  @Test
  void enrichConsumesMetricsOnceThenPassesThrough() throws Exception {
    UnityCatalogCommitter committer = committer("file:/tmp/unused");

    // no metrics supplied -> pass-through: the same iterator instance is returned untouched.
    CloseableIterator<Row> bare = actions(nonCommitInfoRow());
    assertSame(bare, committer.enrich(bare, commitMetadata(operationParams())));

    // supply metrics, enrich once: the commit-info row is swapped (a new instance), then reset.
    committer.setPendingMetrics(7, 1, 70);
    Row ci = SingleAction.createCommitInfoSingleAction(baseCommitInfo(operationParams()).toRow());
    List<Row> first = drain(committer.enrich(actions(ci), commitMetadata(operationParams())));
    assertFalse(first.get(0).isNullAt(SingleAction.COMMIT_INFO_ORDINAL));

    // metrics were consumed: a second call with no new setPendingMetrics is a pass-through again.
    CloseableIterator<Row> again = actions(nonCommitInfoRow());
    assertSame(again, committer.enrich(again, commitMetadata(operationParams())));
    // and the reset is visible: -1 == not supplied.
    assertEquals(-1L, committer.pendingNumRows);
  }

  // ---- catalogState: ascending sort + already-published dedup ----------------------------------

  @Test
  void catalogStateSortsAscendingAndDropsPublishedCommits(@TempDir java.nio.file.Path tableDir) throws Exception {
    String tableUri = tableDir.toUri().toString();
    String logPath = logPath(tableDir);
    // v0 was already backfilled to the published log; Kernel reads it directly, so catalogState must
    // not also return it as staged log data (that would double-count the version).
    java.nio.file.Files.createDirectories(tableDir.resolve("_delta_log"));
    Files.write(tableDir.resolve("_delta_log").resolve(String.format("%020d.json", 0L)),
        "{}".getBytes(StandardCharsets.UTF_8));

    // UC returns commits newest-first.
    List<Commit> newestFirst = new ArrayList<>();
    newestFirst.add(stagedCommit(logPath, 2));
    newestFirst.add(stagedCommit(logPath, 1));
    newestFirst.add(stagedCommit(logPath, 0));
    UnityCatalogCommitter committer =
        new UnityCatalogCommitter(
            "table-id", tableUri, new Configuration(), new FakeUCClient(new GetCommitsResponse(newestFirst, 2)));

    UnityCatalogCommitter.CatalogState state = committer.catalogState();

    assertEquals(2, state.maxVersion);
    List<Long> versions = new ArrayList<>();
    for (ParsedCatalogCommitData c : asCatalog(state.commits)) {
      versions.add(c.getVersion());
    }
    // v0 deduped (already published), and the rest sorted ascending for Kernel's withLogData.
    assertEquals(List.of(1L, 2L), versions);
  }

  // ---- publish: backfill copies staged commits, skips if already published ---------------------

  @Test
  void publishBackfillsStagedCommitsAndSkipsExisting(@TempDir java.nio.file.Path tableDir) throws Exception {
    String logPath = logPath(tableDir);
    java.nio.file.Path logDir = tableDir.resolve("_delta_log");
    Files.createDirectories(logDir);

    // two staged commits to publish; v0's published target already exists (must be left untouched).
    String published0 = String.format("%020d.json", 0L);
    Files.write(logDir.resolve(published0), "ALREADY-PUBLISHED".getBytes(StandardCharsets.UTF_8));
    ParsedCatalogCommitData c0 = stagedData(tableDir, logPath, 0, "staged-0");
    ParsedCatalogCommitData c1 = stagedData(tableDir, logPath, 1, "staged-1");

    UnityCatalogCommitter committer = committer(tableDir.toUri().toString());
    committer.publish(null, new PublishMetadata(1, logPath, List.of(c0, c1)));

    // v0 skip-if-exists: original published bytes preserved, not overwritten with the staged copy.
    assertEquals("ALREADY-PUBLISHED",
        new String(Files.readAllBytes(logDir.resolve(published0)), StandardCharsets.UTF_8));
    // v1 backfilled to its numbered published name with the staged bytes.
    assertEquals("staged-1",
        new String(Files.readAllBytes(logDir.resolve(String.format("%020d.json", 1L))), StandardCharsets.UTF_8));
    // highestPublishedVersion advances to the max version copied (told to UC on the next commit).
    assertEquals(1L, committer.highestPublishedVersion.get());
  }

  // ---- auth failures are non-retryable (fail fast instead of storming) -------------------------

  @Test
  void authFailureDetectedAnywhereInCauseChain() {
    assertTrue(UnityCatalogCommitter.isAuthFailure(new RuntimeException("ApiException: 401 Unauthorized")));
    assertTrue(
        UnityCatalogCommitter.isAuthFailure(
            new RuntimeException("commit failed", new java.io.IOException("Token is expired"))));
    assertTrue(UnityCatalogCommitter.isAuthFailure(new RuntimeException("oidc error: invalid_client")));
    // a real conflict is retryable -- must not be misclassified as auth
    assertFalse(UnityCatalogCommitter.isAuthFailure(new RuntimeException("version 5 already exists; conflict")));
    assertFalse(UnityCatalogCommitter.isAuthFailure(null));
  }

  // ---- fixtures --------------------------------------------------------------------------------

  private static UnityCatalogCommitter committer(String tableStorageLocation) {
    return new UnityCatalogCommitter(
        "https://workspace.example", "token", "table-id", tableStorageLocation, new Configuration());
  }

  private static Map<String, String> operationParams() {
    Map<String, String> p = new HashMap<>();
    p.put("predicate", "[]");
    return p;
  }

  private static CommitInfo baseCommitInfo(Map<String, String> params) {
    return new CommitInfo(
        Optional.empty(), 1000L, Optional.of("kernel"), Optional.of("WRITE"),
        params, Optional.empty(), Optional.of("txn-1"), new HashMap<>());
  }

  // version-0 CommitMetadata: read-state absent, new protocol+metadata present, plain protocol so no
  // in-commit-timestamp is demanded. enrichedCommitInfo only reads getCommitInfo().
  private static CommitMetadata commitMetadata(Map<String, String> params) {
    Metadata meta = new Metadata(
        "tbl", Optional.empty(), Optional.empty(), new Format(),
        "{\"type\":\"struct\",\"fields\":[]}", new StructType(),
        emptyArray(), Optional.empty(), emptyMap());
    return new CommitMetadata(
        0L, "file:/tmp/tbl/_delta_log", baseCommitInfo(params), Collections.emptyList(),
        Collections::emptyMap, Optional.empty(), Optional.of(new Protocol(1, 2)),
        Optional.of(meta), Optional.empty());
  }

  private static Row nonCommitInfoRow() {
    // enrich only inspects isNullAt(COMMIT_INFO_ORDINAL); anything else is forwarded by identity.
    return new StubRow();
  }

  @SafeVarargs
  private static CloseableIterator<Row> actions(Row... rows) {
    return Iters.closeable(List.of(rows).iterator());
  }

  private static List<Row> drain(CloseableIterator<Row> it) throws IOException {
    List<Row> out = new ArrayList<>();
    while (it.hasNext()) {
      out.add(it.next());
    }
    it.close();
    return out;
  }

  private static Commit stagedCommit(String logPath, long version) {
    String name = FileNames.stagedCommitFile(new io.delta.kernel.internal.fs.Path(logPath), version);
    org.apache.hadoop.fs.FileStatus h =
        new org.apache.hadoop.fs.FileStatus(10, false, 1, 64, 1000 + version, new Path(name));
    return new Commit(version, h, 1000 + version);
  }

  // write a real staged commit file under _staged_commits/ so publish can copy its bytes.
  private static ParsedCatalogCommitData stagedData(
      java.nio.file.Path tableDir, String logPath, long version, String contents) throws IOException {
    String name = FileNames.stagedCommitFile(new io.delta.kernel.internal.fs.Path(logPath), version);
    java.nio.file.Path file = java.nio.file.Paths.get(URI.create(name));
    Files.createDirectories(file.getParent());
    Files.write(file, contents.getBytes(StandardCharsets.UTF_8));
    return ParsedCatalogCommitData.forFileStatus(
        io.delta.kernel.utils.FileStatus.of(name, contents.length(), 1000 + version));
  }

  private static String logPath(java.nio.file.Path tableDir) {
    String base = tableDir.toUri().toString();
    return (base.endsWith("/") ? base : base + "/") + "_delta_log";
  }

  @SuppressWarnings("unchecked")
  private static List<ParsedCatalogCommitData> asCatalog(List<?> commits) {
    return (List<ParsedCatalogCommitData>) commits;
  }

  // ---- minimal Kernel stubs --------------------------------------------------------------------

  private static ArrayValue emptyArray() {
    return new ArrayValue() {
      @Override
      public int getSize() {
        return 0;
      }

      @Override
      public ColumnVector getElements() {
        return null;
      }
    };
  }

  private static MapValue emptyMap() {
    return new MapValue() {
      @Override
      public int getSize() {
        return 0;
      }

      @Override
      public ColumnVector getKeys() {
        return null;
      }

      @Override
      public ColumnVector getValues() {
        return null;
      }
    };
  }

  // an action Row that carries no commit-info; enrich must forward it unchanged.
  private static final class StubRow implements Row {
    @Override
    public StructType getSchema() {
      return SingleAction.FULL_SCHEMA;
    }

    @Override
    public boolean isNullAt(int ordinal) {
      return true;
    }

    @Override
    public boolean getBoolean(int ordinal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte getByte(int ordinal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(int ordinal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(int ordinal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int ordinal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(int ordinal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int ordinal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getString(int ordinal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public java.math.BigDecimal getDecimal(int ordinal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBinary(int ordinal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Row getStruct(int ordinal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public io.delta.kernel.data.ArrayValue getArray(int ordinal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public io.delta.kernel.data.MapValue getMap(int ordinal) {
      throw new UnsupportedOperationException();
    }
  }

  // returns a canned getCommits response; every other UCClient call is unused offline.
  private static final class FakeUCClient implements UCClient {
    private final GetCommitsResponse response;

    FakeUCClient(GetCommitsResponse response) {
      this.response = response;
    }

    @Override
    public GetCommitsResponse getCommits(
        String tableId, URI tableUri, Optional<Long> startVersion, Optional<Long> endVersion) {
      return response;
    }

    @Override
    public String getMetastoreId() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void commit(
        String tableId,
        URI tableUri,
        Optional<Commit> commit,
        Optional<Long> lastKnownBackfilledVersion,
        boolean disownCommit,
        Optional<io.delta.storage.commit.actions.AbstractMetadata> metadata,
        Optional<io.delta.storage.commit.actions.AbstractProtocol> protocol,
        Optional<io.delta.storage.commit.uniform.UniformMetadata> uniform) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void finalizeCreate(
        String a, String b, String c, String d,
        List<UCClient.ColumnDef> cols, Map<String, String> props) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {}
  }
}
