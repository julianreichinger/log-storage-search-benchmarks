package com.dynatrace.index;

import static com.dynatrace.index.storage.PostFiltering.readAndPostFilterLogs;
import static java.util.Objects.requireNonNull;

import com.dynatrace.index.data.analysis.tokenization.Lowercase;
import com.dynatrace.index.storage.BatchReader;
import com.dynatrace.index.storage.LogConsumer;
import java.util.BitSet;

/**
 * Base class for all implementations pre-filtering data through some indexing structure.
 */
public abstract class LogStoreReaderBase implements LogStoreReader {

  private final BatchReader reader;
  private final BitSet matchingBatches;

  protected LogStoreReaderBase(BatchReader reader) {
    this.reader = requireNonNull(reader);
    this.matchingBatches = new BitSet();
  }

  @Override
  public void queryToken(byte[] utf8Token, LogConsumer logConsumer, QueryTrace trace, boolean loadData) {
    matchingBatches.clear();

    final byte[] lowerCaseToken = new byte[utf8Token.length];
    Lowercase.toLowerCase(utf8Token, 0, utf8Token.length, lowerCaseToken);

    locateTokenBatches(lowerCaseToken, matchingBatches);
    if (loadData) {
      readAndPostFilterLogs(reader, lowerCaseToken, matchingBatches, logConsumer, trace);
    }
  }

  @Override
  public void queryContains(byte[] utf8String, LogConsumer logConsumer, QueryTrace trace, boolean loadData) {
    matchingBatches.clear();

    final byte[] lowerCaseToken = new byte[utf8String.length];
    Lowercase.toLowerCase(utf8String, 0, utf8String.length, lowerCaseToken);

    locateContainsBatches(lowerCaseToken, matchingBatches);

    if (loadData) {
      readAndPostFilterLogs(reader, lowerCaseToken, matchingBatches, logConsumer, trace);
    }
  }

  @Override
  public void close() {
    reader.close();
  }

  protected abstract void locateTokenBatches(byte[] utf8Token, BitSet batches);

  protected abstract void locateContainsBatches(byte[] utf8String, BitSet batches);
}
