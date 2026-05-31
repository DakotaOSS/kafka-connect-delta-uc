package io.dakotaoss.delta.util;

import java.util.regex.Pattern;

/**
 * Strip secrets from text before it reaches logs or exception messages. Hadoop's ABFS exceptions
 * embed the request URL including the SAS query string, and UC error bodies can carry vended
 * credentials; both flow into Connect's task-failure logging if passed through verbatim.
 */
public final class Redact {

  private Redact() {}

  // Whole abfss:// URL. Mask authority + path + query, not just the SAS query string: the path
  // discloses the storage account / container layout, and the SAS lives in the query. One rule
  // covers both; prefer logging the UC catalog.schema.table name where we control the message.
  private static final Pattern ABFS_URL = Pattern.compile("(?i)abfss?://[^\\s\"'<]+");
  // loose SAS / bearer fragments anywhere
  private static final Pattern SAS_PARAMS =
      Pattern.compile("(?i)(sig|se|st|skoid|sktid|sks|ske|skt|skv|spr|srt|ss|sp|sr|sv|sdd)=[^&\\s\"']+");
  private static final Pattern BEARER = Pattern.compile("(?i)(bearer\\s+)[A-Za-z0-9._\\-]+");
  private static final Pattern SAS_TOKEN_JSON =
      Pattern.compile("(?i)(\"?sas_token\"?\\s*[:=]\\s*\"?)[^\"&\\s,}]+");

  /** Return {@code s} with abfss:// URLs, SAS params, bearer tokens, and sas_token values masked. */
  public static String text(String s) {
    if (s == null) {
      return null;
    }
    s = ABFS_URL.matcher(s).replaceAll("abfss://<redacted>");
    s = SAS_PARAMS.matcher(s).replaceAll("$1=<redacted>");
    s = SAS_TOKEN_JSON.matcher(s).replaceAll("$1<redacted>");
    s = BEARER.matcher(s).replaceAll("$1<redacted>");
    return s;
  }

  /** Redacted {@code toString()} of a throwable, for safe logging. */
  public static String message(Throwable t) {
    return t == null ? null : text(t.toString());
  }
}
