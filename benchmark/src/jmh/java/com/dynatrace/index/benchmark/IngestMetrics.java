package com.dynatrace.index.benchmark;

import com.dynatrace.index.IngestTrace;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Secondary metrics which are measured once per created index.
 * <p>
 * NOTE: JMH summarizes the values over all iterations in the output (but does not include the warmup iterations),
 * so the results have to be divided by the iteration count.
 */
@State(Scope.Thread)
@AuxCounters(value = AuxCounters.Type.EVENTS)
public class IngestMetrics implements IngestTrace {

  private long sourceCount;
  private long lineCount;
  private long tokenCount;

  @Setup(Level.Iteration)
  public void clean() {
    sourceCount = 0;
    lineCount = 0;
    tokenCount = 0;
  }

  @Override
  public void trackIngestedLine(int sourceId, int tokens) {
    this.lineCount++;
    this.sourceCount = Math.max(sourceCount, sourceId + 1);
    this.tokenCount += tokens;
  }

  public long sourceCount() {
    return sourceCount;
  }

  public long lineCount() {
    return lineCount;
  }

  public long tokenCount() {
    return tokenCount;
  }
}
