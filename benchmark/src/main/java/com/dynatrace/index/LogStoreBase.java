package com.dynatrace.index;

import static com.dynatrace.index.util.FileUtils.directorySize;
import static java.util.Objects.requireNonNull;

import com.dynatrace.index.data.analysis.tokenization.Lowercase;
import com.dynatrace.index.data.analysis.tokenization.Tokenizer;
import com.dynatrace.index.storage.BatchWriter;
import com.dynatrace.index.tokenization.BulkTokenConsumer;
import com.dynatrace.index.tokenization.IngestTokenSink;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import javax.annotation.Nullable;

/**
 * Base class for all implementations pre-filtering data through some indexing structure.
 */
public abstract class LogStoreBase extends LogStoreReaderBase implements LogStore {

  private static final int BATCH_SIZE_SHIFT = 19; // x >> 19 => x / 512k

  private final BatchWriter batchWriter;
  private final Tokenizer ingestTokenizer;
  private final IngestTokenSink tokenSink;
  private final int maxBatchCount;
  private final Path dataDirectory;

  private long[] sourceSizes;
  private byte[] lowercase;

  protected LogStoreBase(
      BatchWriter batchWriter,
      Path dataDirectory,
      Tokenizer tokenizer,
      BulkTokenConsumer tokenConsumer,
      int maxBatchCount) {
    super(batchWriter);

    this.batchWriter = requireNonNull(batchWriter);
    this.dataDirectory = requireNonNull(dataDirectory);
    this.maxBatchCount = maxBatchCount;
    this.ingestTokenizer = requireNonNull(tokenizer);
    this.tokenSink = new IngestTokenSink(tokenConsumer);
    this.lowercase = new byte[16 * 1024];
    this.sourceSizes = new long[4096];
  }

  @Override
  public void addLogLine(byte[] bytes, int offset, int length, int sourceId, @Nullable IngestTrace trace) {
    final int batch = getBatch(sourceId, length);

    // Store data
    batchWriter.addLogLine(bytes, offset, length, batch);

    // Index data
    if (length > lowercase.length) {
      int newSize = Math.max(length, lowercase.length * 2);
      lowercase = new byte[newSize];
    }
    Lowercase.toLowerCase(bytes, offset, length, lowercase);
    tokenSink.startLine(lowercase, batch);
    ingestTokenizer.tokenize(lowercase, 0, length, tokenSink);
    final int tokenCount = tokenSink.getTokenCount();
    tokenSink.endLine();

    if (trace != null) {
      trace.trackIngestedLine(sourceId, tokenCount);
    }
  }

  @Override
  public void finish(FinishTrace trace) throws IOException {
    final long start = System.nanoTime();
    batchWriter.flush();
    trace.trackDataFinishTime(System.nanoTime() - start);
    trace.trackDataDiskUsage(directorySize(dataDirectory));
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
