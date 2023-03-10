package com.dynatrace.index.scan;

import static com.dynatrace.index.scan.ScanLogStoreReader.scanQuery;
import static com.dynatrace.index.storage.StorageDirectories.dataDirectory;
import static com.dynatrace.index.util.FileUtils.directorySize;

import com.dynatrace.index.FinishTrace;
import com.dynatrace.index.IngestTrace;
import com.dynatrace.index.LogStore;
import com.dynatrace.index.QueryTrace;
import com.dynatrace.index.storage.BatchWriter;
import com.dynatrace.index.storage.DefaultBatchWriter;
import com.dynatrace.index.storage.LogConsumer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Arrays;
import javax.annotation.Nullable;

/**
 * Simple LogStore implementation which uses z-standard to compress data and always performs a full scan search
 * for queries.
 */
public final class ScanLogStore implements LogStore {

  private static final int BATCH_SIZE_SHIFT = 19; // x >> 19 => x / 512k

  private final Path dataDirectory;
  private final BatchWriter batchWriter;
  private final int maxBatchCount;

  private long[] sourceSizes;

  public ScanLogStore(Path storageDirectory, int maxBatchCount) {
    this.dataDirectory = dataDirectory(storageDirectory);
    this.batchWriter = new DefaultBatchWriter(dataDirectory);
    this.maxBatchCount = maxBatchCount;
    this.sourceSizes = new long[4096];
  }

  @Override
  public void addLogLine(byte[] bytes, int offset, int length, int sourceId, @Nullable IngestTrace trace) {
    final int batch = getBatch(sourceId, length);
    batchWriter.addLogLine(bytes, offset, length, batch);

    // Does not produce tokens
    if (trace != null) {
      trace.trackIngestedLine(sourceId, 0);
    }
  }

  @Override
  public void queryToken(byte[] utf8Token, LogConsumer logConsumer, QueryTrace trace, boolean loadData) {
    scanQuery(batchWriter, utf8Token, logConsumer, trace);
  }

  @Override
  public void queryContains(byte[] utf8String, LogConsumer logConsumer, QueryTrace trace, boolean loadData) {
    scanQuery(batchWriter, utf8String, logConsumer, trace);
  }

  @Override
  public long estimatedMemoryUsageBytes() {
    return 0;
  }

  @Override
  public void finish(FinishTrace trace) {
    // Write data
    try {
      final long start = System.nanoTime();
      batchWriter.flush();
      trace.trackDataFinishTime(System.nanoTime() - start);
      trace.trackDataDiskUsage(directorySize(dataDirectory));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void close() {
    batchWriter.close();
  }

  private int getBatch(int sourceId, int length) {
    if (sourceId >= sourceSizes.length) {
      int newSize = Math.max(sourceId + 1, sourceSizes.length * 2);
      sourceSizes = Arrays.copyOf(sourceSizes, newSize);
    }

    // Randomize the start position of the source to get a relatively even data distribution
    // Sources which are too big "overflow" to the next batch(es)
    int sourceStart = (sourceId * 0x915f77f5 + 13) & 0x7fffffff;
    long sourceSize = sourceSizes[sourceId];
    long sourceBatch = sourceSize >> BATCH_SIZE_SHIFT;
    sourceSizes[sourceId] = sourceSize + length;
    return (int) ((sourceStart + sourceBatch) % maxBatchCount);
  }
}
