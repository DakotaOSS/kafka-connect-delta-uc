package io.dakotaoss.delta;

import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Background sampler for the benchmarks: peak JVM heap, avg/peak process CPU, peak thread count.
 * This is the in-JVM view; the run scripts also capture the cgroup view via {@code docker stats},
 * because the two disagree (heap != RSS, JVM CPU != container CPU under throttling).
 */
final class BenchResourceSampler implements AutoCloseable {

  private final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
  private final OperatingSystemMXBean os =
      (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
  private final MemoryMXBean mem = ManagementFactory.getMemoryMXBean();
  private final ThreadMXBean threads = ManagementFactory.getThreadMXBean();

  private long peakHeap;
  private double cpuSum;
  private int cpuSamples;
  private double peakCpu;
  private int peakThreads;

  BenchResourceSampler(long periodMs) {
    exec.scheduleAtFixedRate(this::sample, 0, periodMs, TimeUnit.MILLISECONDS);
  }

  private synchronized void sample() {
    peakHeap = Math.max(peakHeap, mem.getHeapMemoryUsage().getUsed());
    double cpu = os.getProcessCpuLoad(); // [0,1]; <0 until the JVM has a measurement window
    if (cpu >= 0) {
      cpuSum += cpu;
      cpuSamples++;
      peakCpu = Math.max(peakCpu, cpu);
    }
    peakThreads = Math.max(peakThreads, threads.getThreadCount());
  }

  synchronized double peakHeapMb() {
    return peakHeap / (1024.0 * 1024.0);
  }

  synchronized double avgCpuPct() {
    return cpuSamples == 0 ? -1 : 100.0 * cpuSum / cpuSamples;
  }

  synchronized double peakCpuPct() {
    return 100.0 * peakCpu;
  }

  synchronized int peakThreads() {
    return peakThreads;
  }

  @Override
  public void close() {
    exec.shutdownNow();
  }
}
