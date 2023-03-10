package com.dynatrace.index.benchmark;

import com.dynatrace.index.QueryTrace;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Secondary metrics about query execution which are measured once query benchmark iteration.
 *
 * <p>NOTE: JMH summarizes the values over all iterations in the output (but does not include the warmup iterations),
 * so the results have to be divided by the iteration count.
 */
@State(Scope.Thread)
@AuxCounters(value = AuxCounters.Type.EVENTS)
public class QueryMetrics implements QueryTrace {

  private long falsePositives;
  private long truePositives;
  private long batches;
  private long queryCount;

  @Setup(Level.Iteration)
  public void clean() {
    falsePositives = 0;
    truePositives = 0;
    batches = 0;
    queryCount = 0;
  }

  @Override
  public void trackErrorRate(int falsePositives, int truePositives, int batches) {
    this.falsePositives += falsePositives;
    this.truePositives += truePositives;
    this.batches += batches;
    this.queryCount++;
  }

  public double errorRate() {
    return batches == 0 ? 0 : ((double) falsePositives) / batches;
  }

  public long falsePositives() {
    return falsePositives;
  }

  public long truePositives() {
    return truePositives;
  }

  public long batches() {
    return batches;
  }

  public long queryCount() {
    return queryCount;
  }
}
