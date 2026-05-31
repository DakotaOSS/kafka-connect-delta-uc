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
        "sigA", store.sasFor("acct.dfs.core.windows.net", "cont", "__unitystorage/tables/A/_delta_log/00.json"));
    // exact-dir request also matches
    assertEquals("sigA", store.sasFor("acct.dfs.core.windows.net", "cont", "__unitystorage/tables/A"));
  }

  @Test
  void isolatesTwoTablesSharingOneAccountAndContainer() {
    // the #4 scenario: two tables on the same storage host (one cached FileSystem) must each get their
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
    store.put("rddakotadbstorageunity.dfs.core.windows.net", "cont", "ns/tables/A", "sig".toCharArray());
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
    // "ns/tables/A" must not match a request for "ns/tables/AB/..." (string prefix but not a path prefix)
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
}
