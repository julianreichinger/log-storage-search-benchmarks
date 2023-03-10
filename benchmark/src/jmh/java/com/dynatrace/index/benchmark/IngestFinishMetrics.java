package com.dynatrace.index.benchmark;

import com.dynatrace.index.FinishTrace;
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
public class IngestFinishMetrics implements FinishTrace {

  private double sketchMemoryUsageMB;
  private double sketchDiskUsageMB;
  private double dataDiskUsageMB;
  private double sketchFinishTimeSeconds;
  private double dataFinishTimeSeconds;

  @Setup(Level.Iteration)
  public void clean() {
    sketchFinishTimeSeconds = 0;
    dataFinishTimeSeconds = 0;
    sketchMemoryUsageMB = 0;
    sketchDiskUsageMB = 0;
    dataDiskUsageMB = 0;
  }

  @Override
  public void trackSketchMemoryUsage(long memoryBytes) {
    this.sketchMemoryUsageMB = memoryBytes / 1024d / 1024d;
  }

  @Override
  public void trackSketchFinishTime(long nanoseconds) {
    sketchFinishTimeSeconds = nanoseconds / 1_000_000_000D;
  }

  @Override
  public void trackDataFinishTime(long nanoseconds) {
    dataFinishTimeSeconds = nanoseconds / 1_000_000_000D;
  }

  @Override
  public void trackSketchDiskUsage(long sketchDiskBytes) {
    this.sketchDiskUsageMB = sketchDiskBytes / 1024d / 1024d;
  }

  @Override
  public void trackDataDiskUsage(long dataDiskBytes) {
    this.dataDiskUsageMB = dataDiskBytes / 1024d / 1024d;
  }

  public double sketchMemoryUsage() {
    return sketchMemoryUsageMB;
  }

  public double sketchDiskUsage() {
    return sketchDiskUsageMB;
  }

  public double dataDiskUsage() {
    return dataDiskUsageMB;
  }

  public double sketchFinishTimeSeconds() {
    return sketchFinishTimeSeconds;
  }

  public double dataFinishTimeSeconds() {
    return dataFinishTimeSeconds;
  }
}
