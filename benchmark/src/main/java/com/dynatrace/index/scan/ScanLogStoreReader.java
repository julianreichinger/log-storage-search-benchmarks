package com.dynatrace.index.scan;

import static com.dynatrace.index.storage.PostFiltering.readAllAndPostFilterLogs;
import static com.dynatrace.index.storage.StorageDirectories.dataDirectory;
import static java.util.Objects.requireNonNull;

import com.dynatrace.index.LogStoreReader;
import com.dynatrace.index.QueryTrace;
import com.dynatrace.index.data.analysis.tokenization.Lowercase;
import com.dynatrace.index.storage.BatchReader;
import com.dynatrace.index.storage.DefaultBatchReader;
import com.dynatrace.index.storage.LogConsumer;
import java.nio.file.Path;

/**
 * Simple LogStore implementation which uses z-standard to compress data and always performs a full scan search
 * for queries.
 */
public class ScanLogStoreReader implements LogStoreReader {

  private final BatchReader reader;

  ScanLogStoreReader(BatchReader reader) {
    this.reader = requireNonNull(reader);
  }

  @Override
  public void queryToken(byte[] utf8Token, LogConsumer logConsumer, QueryTrace trace, boolean loadData) {
    scanQuery(reader, utf8Token, logConsumer, trace);
  }

  @Override
  public void queryContains(byte[] utf8String, LogConsumer logConsumer, QueryTrace trace, boolean loadData) {
    scanQuery(reader, utf8String, logConsumer, trace);
  }

  @Override
  public long estimatedMemoryUsageBytes() {
    return 0;
  }

  @Override
  public void close() {
    reader.close();
  }

  public static ScanLogStoreReader loadFromDisk(Path storageDirectory) {
    final Path dataDir = dataDirectory(storageDirectory);
    final BatchReader reader = DefaultBatchReader.create(dataDir);
    return new ScanLogStoreReader(reader);
  }

  static void scanQuery(
      BatchReader reader, byte[] utf8String, LogConsumer logConsumer, QueryTrace trace) {

    final byte[] lowerCaseString = new byte[utf8String.length];
    Lowercase.toLowerCase(utf8String, 0, utf8String.length, lowerCaseString);

    readAllAndPostFilterLogs(reader, lowerCaseString, logConsumer, trace);
  }
}
