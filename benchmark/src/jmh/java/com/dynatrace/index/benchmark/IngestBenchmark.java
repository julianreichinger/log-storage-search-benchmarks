package com.dynatrace.index.benchmark;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;

public class IngestBenchmark {

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Fork(value = 1, jvmArgs = {"-Xms4096m", "-Xmx16g", "-Djdk.attach.allowAttachSelf=true"})
  @Warmup(iterations = 3, batchSize = 10)
  @Measurement(iterations = 3, batchSize = 1_000)
  @Timeout(time = 3, timeUnit = TimeUnit.HOURS)
  public void ingest(IngestState ingestState) {
    ingestState.indexBatch();
  }
}
