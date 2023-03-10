package com.dynatrace.index.benchmark;

import static com.dynatrace.index.benchmark.QueryFunction.executeContainsQuery;
import static com.dynatrace.index.benchmark.QueryFunction.executeTokenQuery;

import com.dynatrace.index.LogStoreReader;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1, jvmArgs = {"-Xms4096m", "-Xmx16g", "-Djdk.attach.allowAttachSelf=true"})
@Warmup(time = 60, iterations = 1)
@Measurement(time = 60, iterations = 1)
@Timeout(time = 3, timeUnit = TimeUnit.HOURS)
public class QueryBenchmark {

  @Benchmark
  public void mixedQuery(QueryState queryState, QueryMetrics queryMetrics, Blackhole blackhole) {
    final LogStoreReader reader = queryState.acquireReader();
    // Retrieves either an indexed or an unknown token
    final byte[] queryToken = queryState.nextMixedQueryToken();


    final float queryRandom = ThreadLocalRandom.current().nextFloat();
    if (queryRandom < 0.5) {
      executeTokenQuery(reader, queryToken, queryMetrics, queryState.isLoadData(), blackhole);
    } else {
      executeContainsQuery(reader, queryToken, queryMetrics, queryState.isLoadData(), blackhole);
    }
  }

  @Benchmark
  public void tokenQuery(QueryState queryState, QueryMetrics queryMetrics, Blackhole blackhole) {
    final LogStoreReader reader = queryState.acquireReader();
    final byte[] queryToken = queryState.nextIndexedToken();
    executeTokenQuery(reader, queryToken, queryMetrics, queryState.isLoadData(), blackhole);
  }

  @Benchmark
  public void containsQuery(QueryState queryState, QueryMetrics queryMetrics, Blackhole blackhole) {
    final LogStoreReader reader = queryState.acquireReader();
    final byte[] queryToken = queryState.nextIndexedToken();
    executeContainsQuery(reader, queryToken, queryMetrics, queryState.isLoadData(), blackhole);
  }

  @Benchmark
  public void unknownIdQuery(QueryState queryState, QueryMetrics queryMetrics, Blackhole blackhole) {
    final LogStoreReader reader = queryState.acquireReader();
    final byte[] queryToken = queryState.nextRandomID();
    executeTokenQuery(reader, queryToken, queryMetrics, queryState.isLoadData(), blackhole);
  }

  @Benchmark
  public void unknownIdContainsQuery(QueryState queryState, QueryMetrics queryMetrics, Blackhole blackhole) {
    final LogStoreReader reader = queryState.acquireReader();
    final byte[] queryToken = queryState.nextRandomID();
    executeContainsQuery(reader, queryToken, queryMetrics, queryState.isLoadData(), blackhole);
  }

  @Benchmark
  public void unknownIpQuery(QueryState queryState, QueryMetrics queryMetrics, Blackhole blackhole) {
    final LogStoreReader reader = queryState.acquireReader();
    final byte[] queryToken = queryState.nextRandomIP();
    executeTokenQuery(reader, queryToken, queryMetrics, queryState.isLoadData(), blackhole);
  }

  @Benchmark
  public void unknownIpContainsQuery(QueryState queryState, QueryMetrics queryMetrics, Blackhole blackhole) {
    final LogStoreReader reader = queryState.acquireReader();
    final byte[] queryToken = queryState.nextRandomIP();
    executeContainsQuery(reader, queryToken, queryMetrics, queryState.isLoadData(), blackhole);
  }
}
