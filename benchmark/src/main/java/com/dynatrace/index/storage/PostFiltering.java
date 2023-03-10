package com.dynatrace.index.storage;

import com.dynatrace.index.QueryTrace;
import java.util.BitSet;
import java.util.PrimitiveIterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

/**
 * Helper class for reading data from a {@link BatchReader} and to post-filter the resulting log lines.
 */
public final class PostFiltering {

  private PostFiltering() {
    // static helper
  }

  /**
   * Read all log lines from the provided {@link BatchReader} and filter lines based on the provided token.
   */
  public static void readAllAndPostFilterLogs(
      BatchReader reader, byte[] utf8Token, LogConsumer consumer, QueryTrace trace) {

    readAndPostFilterLogs(reader, utf8Token, consumer, trace, IntStream.range(0, reader.getMaxBatch() + 1).iterator());
  }

  /**
   * Read all log lines within the matching batches from the provided {@link BatchReader}
   * and filter lines based on the provided token.
   */
  public static void readAndPostFilterLogs(
      BatchReader reader, byte[] utf8Token, BitSet matchingBatches, LogConsumer consumer, QueryTrace trace) {

    if (matchingBatches.isEmpty()) {
      trace.trackErrorRate(0, 0, reader.getMaxBatch() + 1);
      return;
    }

    readAndPostFilterLogs(reader, utf8Token, consumer, trace, matchingBatches.stream().iterator());
  }

  private static void readAndPostFilterLogs(
      BatchReader reader,
      byte[] utf8Token,
      LogConsumer consumer,
      QueryTrace trace,
      PrimitiveIterator.OfInt matchingBatches) {

    final BoyerMoore matcher = BoyerMoore.createForPattern(utf8Token);
    final AtomicBoolean hasMatch = new AtomicBoolean();
    final LogConsumer filter = (bytes, offset, length) -> {
      if (matcher.matchLowerCase(bytes, offset, length) >= 0) {
        consumer.acceptLog(bytes, offset, length);
        hasMatch.set(true);
      }
    };

    final int batches = reader.getMaxBatch() + 1;
    int falsePositives = 0;
    int truePositives = 0;
    while (matchingBatches.hasNext()) {
      final int batch = matchingBatches.nextInt();
      reader.readBatch(batch, filter);
      if (hasMatch.getAndSet(false)) {
        truePositives++;
      } else {
        // Batch did not contain any matching line
        falsePositives++;
      }
    }

    trace.trackErrorRate(falsePositives, truePositives, batches);
  }
}
