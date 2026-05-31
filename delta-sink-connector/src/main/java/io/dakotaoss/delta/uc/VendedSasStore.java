package io.dakotaoss.delta.uc;

import java.util.Arrays;
import java.util.HashMap;
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

  private static final VendedSasStore INSTANCE = new VendedSasStore();

  public static VendedSasStore instance() {
    return INSTANCE;
  }

  // host (lower-cased) -> ( "container/dir" -> SAS char[] )
  private final Map<String, Map<String, char[]>> byHost = new HashMap<>();

  /**
   * Register (or replace) a table's SAS, scoped to {@code container/dirPath}. Takes ownership of
   * {@code sas}; the previous token for the same path is zeroed.
   */
  public synchronized void put(String host, String container, String dirPath, char[] sas) {
    Map<String, char[]> m = byHost.computeIfAbsent(accountKey(host), h -> new HashMap<>());
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
    Map<String, char[]> m = byHost.get(accountKey(host));
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

  // The storage account, lower-cased and reduced to the bare account name. ABFS passes getSASToken the
  // short account ("acct") while the table URI carries the full host ("acct.dfs.core.windows.net");
  // key on the part before the first dot so registration and lookup agree.
  private static String accountKey(String host) {
    if (host == null) {
      return null;
    }
    String h = host.toLowerCase(Locale.ROOT);
    int dot = h.indexOf('.');
    return dot < 0 ? h : h.substring(0, dot);
  }
}
