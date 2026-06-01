package io.dakotaoss.delta;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.dakotaoss.delta.uc.VendedSasStore;
import org.junit.jupiter.api.Test;

class VendedSasStoreTest {

  @Test
  void vendsTheSasForAPathUnderTheRegisteredTableDir() {
    VendedSasStore store = new VendedSasStore();
    store.put("acct.dfs.core.windows.net", "cont", "__unitystorage/tables/A", "sigA".toCharArray());
    // a request for a blob under the table dir gets that table's SAS
    assertEquals(
        "sigA",
        store.sasFor(
            "acct.dfs.core.windows.net", "cont", "__unitystorage/tables/A/_delta_log/00.json"));
    // exact-dir request also matches
    assertEquals(
        "sigA", store.sasFor("acct.dfs.core.windows.net", "cont", "__unitystorage/tables/A"));
  }

  @Test
  void isolatesTwoTablesSharingOneAccountAndContainer() {
    // the #4 scenario: two tables on the same storage host (one cached FileSystem) must each get
    // their
    // own directory-scoped SAS, by longest-prefix match on the path.
    VendedSasStore store = new VendedSasStore();
    store.put("h", "cont", "ns/tables/A", "sigA".toCharArray());
    store.put("h", "cont", "ns/tables/B", "sigB".toCharArray());
    assertEquals("sigA", store.sasFor("h", "cont", "ns/tables/A/part-0.parquet"));
    assertEquals("sigB", store.sasFor("h", "cont", "ns/tables/B/part-0.parquet"));
  }

  @Test
  void hostMatchIsCaseInsensitivePathIsNot() {
    VendedSasStore store = new VendedSasStore();
    store.put("Acct.DFS.core.windows.net", "cont", "dir", "sig".toCharArray());
    assertEquals("sig", store.sasFor("acct.dfs.core.windows.net", "cont", "dir/x"));
  }

  @Test
  void matchesShortAccountNameAgainstFullHostRegistration() {
    // ABFS calls getSASToken with the short account ("acct"); the table URI registers the full host
    // ("acct.dfs.core.windows.net"). They must resolve to the same key.
    VendedSasStore store = new VendedSasStore();
    store.put(
        "rddakotadbstorageunity.dfs.core.windows.net", "cont", "ns/tables/A", "sig".toCharArray());
    assertEquals(
        "sig", store.sasFor("rddakotadbstorageunity", "cont", "ns/tables/A/_delta_log/0.json"));
  }

  @Test
  void unregisteredLookupReturnsNull() {
    VendedSasStore store = new VendedSasStore();
    store.put("h", "cont", "dir", "sig".toCharArray());
    assertNull(store.sasFor("other", "cont", "dir/x"), "unknown host");
    assertNull(store.sasFor("h", "cont", "elsewhere/x"), "path under no registered dir");
  }

  @Test
  void doesNotPrefixMatchASiblingDirectoryWithASharedNamePrefix() {
    // "ns/tables/A" must not match a request for "ns/tables/AB/..." (string prefix but not a path
    // prefix)
    VendedSasStore store = new VendedSasStore();
    store.put("h", "cont", "ns/tables/A", "sigA".toCharArray());
    assertNull(store.sasFor("h", "cont", "ns/tables/AB/part.parquet"));
  }

  @Test
  void rerendingZeroesThePreviousToken() {
    VendedSasStore store = new VendedSasStore();
    char[] first = "sigOld".toCharArray();
    store.put("h", "cont", "dir", first); // store takes ownership
    store.put("h", "cont", "dir", "sigNew".toCharArray()); // re-vend replaces + zeroes the old
    assertArrayEquals(new char[first.length], first, "previous SAS must be zeroed on re-vend");
    assertEquals("sigNew", store.sasFor("h", "cont", "dir/x"));
  }

  @Test
  void distinctHostsSharingAShortAccountNameDoNotCollide() {
    // same account reached via dfs and blob endpoints: full-host keying must keep them separate so
    // a
    // dfs request can't be served the blob-scoped SAS (endpoint-mismatched -> 403).
    VendedSasStore store = new VendedSasStore();
    store.put("acct.dfs.core.windows.net", "cont", "dir", "sigDfs".toCharArray());
    store.put("acct.blob.core.windows.net", "cont", "dir", "sigBlob".toCharArray());
    assertEquals("sigDfs", store.sasFor("acct.dfs.core.windows.net", "cont", "dir/x"));
    assertEquals("sigBlob", store.sasFor("acct.blob.core.windows.net", "cont", "dir/x"));
  }

  @Test
  void shortAccountFallbackIsAmbiguousWhenTwoHostsShareTheName() {
    // ABFS may call getSASToken with the bare account; if two registered hosts share that short
    // name
    // the fallback can't pick one safely, so it declines rather than risk an endpoint mismatch.
    VendedSasStore store = new VendedSasStore();
    store.put("acct.dfs.core.windows.net", "cont", "dir", "sigDfs".toCharArray());
    store.put("acct.blob.core.windows.net", "cont", "dir", "sigBlob".toCharArray());
    assertNull(store.sasFor("acct", "cont", "dir/x"));
  }

  @Test
  void boundsPerHostEntriesByEvictingTheOldestAndZeroingIt() {
    VendedSasStore store = new VendedSasStore(2);
    char[] oldest = "sigA".toCharArray();
    store.put("h", "cont", "dirA", oldest);
    store.put("h", "cont", "dirB", "sigB".toCharArray());
    store.put("h", "cont", "dirC", "sigC".toCharArray()); // exceeds cap -> evicts dirA
    assertArrayEquals(new char[oldest.length], oldest, "evicted SAS must be zeroed");
    assertNull(store.sasFor("h", "cont", "dirA/x"), "evicted entry is gone");
    assertEquals("sigB", store.sasFor("h", "cont", "dirB/x"));
    assertEquals("sigC", store.sasFor("h", "cont", "dirC/x"));
  }

  @Test
  void rerendingAnExistingPathDoesNotCountAgainstTheBound() {
    // steady-state re-vend of the same paths must not trigger eviction.
    VendedSasStore store = new VendedSasStore(2);
    store.put("h", "cont", "dirA", "a1".toCharArray());
    store.put("h", "cont", "dirB", "b1".toCharArray());
    store.put("h", "cont", "dirA", "a2".toCharArray()); // replace, not a new entry
    assertEquals("a2", store.sasFor("h", "cont", "dirA/x"));
    assertEquals("b1", store.sasFor("h", "cont", "dirB/x"));
  }
}
