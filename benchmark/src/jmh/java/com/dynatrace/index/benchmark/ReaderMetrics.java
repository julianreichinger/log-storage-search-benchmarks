package com.dynatrace.index.benchmark;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Secondary metrics about the index reader which are measured once query benchmark iteration.
 *
 * NOTE: JMH summarizes the values over all iterations in the output (but does not include the warmup iterations),
 * so the results have to be divided by the iteration count.
 */
@State(Scope.Thread)
@AuxCounters(value = AuxCounters.Type.EVENTS)
public class ReaderMetrics {

  private long memoryUsageBytes;

  @Setup(Level.Iteration)
  public void clean() {
    memoryUsageBytes = 0;
  }

  void trackMemoryUsage(long memoryUsageBytes) {
    this.memoryUsageBytes = memoryUsageBytes;
  }

  public long memoryUsage() {
    return memoryUsageBytes;
  }
}
