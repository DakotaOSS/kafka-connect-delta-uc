package io.dakotaoss.delta.uc;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Process-wide registry of vended ABFS SAS tokens, keyed by storage host + the table's path within
 * the account. A custom {@link VendedSasTokenProvider} reads it to hand ABFS each table's
 * directory-scoped SAS at the request boundary.
 *
 * <p>Why a side store instead of the Hadoop {@code Configuration}:
 * <ul>
 *   <li><b>secrecy</b> — the SAS is held here as {@code char[]} (zeroed on re-vend) and never placed
 *       in the {@code Configuration}, so a config dump can't expose the live write credential and the
 *       cleartext can be cleared.
 *   <li><b>per-table isolation without disabling the FS cache</b> — Hadoop caches one FileSystem per
 *       storage host. With the SAS in the config that single FS would carry one table's token and 403
 *       on a second table sharing the host. Vending per request path lets one cached FS serve many
 *       tables, so we no longer disable the JVM-global FS cache.
 * </ul>
 *
 * <p>Instances are independent (tests use their own); production uses {@link #instance()}.
 */
public final class VendedSasStore {

  // a moved storage location or a churning topic/path set could otherwise grow byHost's inner maps
  // without bound (zeroed arrays, no secret leak — just heap); cap each host and evict the LRU entry.
  private static final int DEFAULT_MAX_ENTRIES_PER_HOST = 1024;

  private static final VendedSasStore INSTANCE = new VendedSasStore();

  public static VendedSasStore instance() {
    return INSTANCE;
  }

  private final int maxEntriesPerHost;

  // full host (lower-cased) -> ( "container/dir" -> SAS char[] ), inner map in LRU access order
  private final Map<String, Map<String, char[]>> byHost = new HashMap<>();

  public VendedSasStore() {
    this(DEFAULT_MAX_ENTRIES_PER_HOST);
  }

  // visible for tests that drive eviction with a small cap
  public VendedSasStore(int maxEntriesPerHost) {
    this.maxEntriesPerHost = maxEntriesPerHost;
  }

  /**
   * Register (or replace) a table's SAS, scoped to {@code container/dirPath}. Takes ownership of
   * {@code sas}; the previous token for the same path is zeroed. Evicting the LRU entry once the host
   * exceeds its entry cap also zeroes the dropped token.
   */
  public synchronized void put(String host, String container, String dirPath, char[] sas) {
    Map<String, char[]> m = byHost.computeIfAbsent(hostKey(host), h -> newBoundedMap());
    char[] prev = m.put(path(container, dirPath), sas);
    if (prev != null && prev != sas) {
      Arrays.fill(prev, '\0');
    }
  }

  /**
   * The SAS for the registered table directory that is the longest path-prefix of
   * {@code container/path}, as a freshly materialized String (the HTTP boundary), or {@code null} if
   * none is registered.
   */
  public synchronized String sasFor(String host, String container, String path) {
    Map<String, char[]> m = mapFor(host);
    if (m == null) {
      return null;
    }
    String key = path(container, path);
    String best = null;
    for (String prefix : m.keySet()) {
      // path-prefix, not string-prefix: "ns/A" must not match "ns/AB/..."
      if (key.equals(prefix) || key.startsWith(prefix + "/")) {
        if (best == null || prefix.length() > best.length()) {
          best = prefix;
        }
      }
    }
    return best == null ? null : new String(m.get(best));
  }

  private Map<String, char[]> newBoundedMap() {
    return new LinkedHashMap<>(16, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<String, char[]> eldest) {
        if (size() <= maxEntriesPerHost) {
          return false;
        }
        Arrays.fill(eldest.getValue(), '\0');
        return true;
      }
    };
  }

  // Resolve the inner map for a lookup host. Registration always stores the full host; ABFS, however,
  // may call getSASToken with either the full host or the bare account name. Prefer an exact full-host
  // match; only when the lookup is a bare account (no dot) fall back to a registered host whose short
  // name matches — and only if exactly one does, so dfs/blob/private-link hosts that share a short name
  // can't be served each other's endpoint-mismatched SAS.
  private Map<String, char[]> mapFor(String host) {
    if (host == null) {
      return null;
    }
    String h = host.toLowerCase(Locale.ROOT);
    Map<String, char[]> exact = byHost.get(h);
    if (exact != null || h.indexOf('.') >= 0) {
      return exact;
    }
    Map<String, char[]> hit = null;
    for (Map.Entry<String, Map<String, char[]>> e : byHost.entrySet()) {
      if (shortName(e.getKey()).equals(h)) {
        if (hit != null) {
          return null; // ambiguous short name -> decline rather than risk a mismatch
        }
        hit = e.getValue();
      }
    }
    return hit;
  }

  private static String hostKey(String host) {
    return host == null ? null : host.toLowerCase(Locale.ROOT);
  }

  private static String shortName(String host) {
    int dot = host.indexOf('.');
    return dot < 0 ? host : host.substring(0, dot);
  }

  private static String path(String container, String p) {
    return strip(container) + "/" + strip(p);
  }

  private static String strip(String s) {
    if (s == null) {
      return "";
    }
    int a = 0;
    int b = s.length();
    while (a < b && s.charAt(a) == '/') {
      a++;
    }
    while (b > a && s.charAt(b - 1) == '/') {
      b--;
    }
    return s.substring(a, b);
  }
}
