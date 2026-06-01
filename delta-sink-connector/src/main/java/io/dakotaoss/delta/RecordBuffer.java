package io.dakotaoss.delta;

import io.dakotaoss.delta.schema.RecordSizeEstimator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Per-partition flush buffer: the rows queued for each topic-partition plus the running byte total and
 * first-arrival timestamp the byte/interval flush dials read. Methods are {@code synchronized} so the
 * map structure is safe when {@code flush.concurrency>1} runs per-table commit tasks that clear
 * different partitions in parallel; the WAN commit happens outside these methods, so locking the buffer
 * never serializes a commit. A given partition's row list is only handled by its own table's task.
 */
final class RecordBuffer {

  private final Map<TopicPartition, List<SinkRecord>> rows = new HashMap<>();
  private final Map<TopicPartition, Long> startMs = new HashMap<>();
  private final Map<TopicPartition, Long> bytes = new HashMap<>();
  // estimate bytes only when flush.bytes is enabled -- the estimate isn't free, and with the dial off
  // nothing reads it.
  private final boolean trackBytes;

  RecordBuffer(boolean trackBytes) {
    this.trackBytes = trackBytes;
  }

  /** Total rows across all partitions -- the figure the backpressure ceiling is checked against. */
  synchronized int totalRows() {
    int n = 0;
    for (List<SinkRecord> b : rows.values()) {
      n += b.size();
    }
    return n;
  }

  synchronized void add(SinkRecord record, long nowMs) {
    TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
    rows.computeIfAbsent(tp, k -> new ArrayList<>()).add(record);
    startMs.putIfAbsent(tp, nowMs);
    if (trackBytes) {
      bytes.merge(tp, RecordSizeEstimator.estimate(record), Long::sum);
    }
  }

  /** The partition's queued rows (an empty list after a clear, null if never seen). */
  synchronized List<SinkRecord> rows(TopicPartition tp) {
    return rows.get(tp);
  }

  synchronized long byteSize(TopicPartition tp) {
    return bytes.getOrDefault(tp, 0L);
  }

  /** Wall-clock of the first record buffered for this partition since the last clear. */
  synchronized long startMs(TopicPartition tp) {
    return startMs.getOrDefault(tp, 0L);
  }

  /** Snapshot of partitions with state, safe to iterate while {@link #clear} mutates the buffer. */
  synchronized List<TopicPartition> partitions() {
    return new ArrayList<>(rows.keySet());
  }

  /** True if this partition has tripped the size or byte dial (each ignored when {@code <= 0}). */
  synchronized boolean tripped(TopicPartition tp, int flushSize, long flushBytes) {
    List<SinkRecord> b = rows.get(tp);
    if (b == null) {
      return false;
    }
    boolean bySize = flushSize > 0 && b.size() >= flushSize;
    boolean byBytes = flushBytes > 0 && byteSize(tp) >= flushBytes;
    return bySize || byBytes;
  }

  /** After a partition's buffer commits: empty its rows (keeping the key) and drop its counters. */
  synchronized void clear(TopicPartition tp) {
    List<SinkRecord> b = rows.get(tp);
    if (b != null) {
      b.clear();
    }
    startMs.remove(tp);
    bytes.remove(tp);
  }

  synchronized void clearAll() {
    rows.clear();
    startMs.clear();
    bytes.clear();
  }
}
