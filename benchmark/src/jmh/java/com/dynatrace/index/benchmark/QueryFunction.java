package com.dynatrace.index.benchmark;

import com.dynatrace.index.LogStoreReader;
import com.dynatrace.index.QueryTrace;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Helper interface to define benchmark queries via method handles.
 */
interface QueryFunction {

  default void run(LogStoreReader reader, byte[] token, QueryTrace trace, Blackhole blackhole) {
    run(reader, token, trace, true, blackhole);
  }

  void run(LogStoreReader reader, byte[] token, QueryTrace trace, boolean loadData, Blackhole blackhole);

  static void executeTokenQuery(
      LogStoreReader reader, byte[] token, QueryTrace trace, boolean loadData, Blackhole blackhole) {
    reader.queryToken(token, (bytes, offset, length) -> blackhole.consume(bytes), trace, loadData);
  }

  static void executeContainsQuery(
      LogStoreReader reader, byte[] token, QueryTrace trace, boolean loadData, Blackhole blackhole) {
    reader.queryContains(token, (bytes, offset, length) -> blackhole.consume(bytes), trace, loadData);
  }
}
