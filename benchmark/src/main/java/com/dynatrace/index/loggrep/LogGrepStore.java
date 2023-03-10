package com.dynatrace.index.loggrep;

import static com.dynatrace.index.loggrep.LogGrepStoreReader.QUERY_BINARY;
import static com.dynatrace.index.loggrep.LogGrepStoreReader.queryDirectory;
import static com.dynatrace.index.storage.StorageDirectories.dataDirectory;
import static com.dynatrace.index.util.FileUtils.directorySize;

import com.dynatrace.index.FinishTrace;
import com.dynatrace.index.IngestTrace;
import com.dynatrace.index.LogStore;
import com.dynatrace.index.QueryTrace;
import com.dynatrace.index.storage.LogConsumer;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

/**
 * LogStore implementation using the <a href="https://dl.acm.org/doi/10.1145/3552326.3567484">LogGrep tool</a>
 * developed by Junyu Wei et.al.
 */
public class LogGrepStore implements LogStore {

  static final String COMPRESSOR_BINARY = "THULR";
  static final String COMPRESSED_DIR = "compressed";

  private static final String TMP_DIR = "tmp";

  private final String compressorBinary;
  private final String queryBinary;
  private final Path tempDir;
  private final Path compressedDir;
  private final Path batchFile;
  private final int maxBatchSize;
  private final List<Path> compressedBatches;
  private final byte[] lineBuffer;

  @Nullable
  private OutputStream outputStream;
  private long batchLines;

  public LogGrepStore(Path binaryDir, Path storageDir, int maxBatchSize) {
    this.compressorBinary = binaryDir.resolve(COMPRESSOR_BINARY).toString();
    this.queryBinary = binaryDir.resolve(QUERY_BINARY).toString();
    Path dataDir = dataDirectory(storageDir);
    this.tempDir = dataDir.resolve(TMP_DIR);
    this.batchFile = tempDir.resolve("batch");
    this.maxBatchSize = maxBatchSize;
    this.compressedDir = dataDir.resolve(COMPRESSED_DIR);
    this.compressedBatches = new ArrayList<>();
    // Make the query line buffer large enough to hold every possible log line for simplicity
    this.lineBuffer = new byte[64 * 1024];
  }

  @Override
  public void addLogLine(byte[] bytes, int offset, int length, int sourceId, @Nullable IngestTrace trace) {
    try {
      OutputStream out = acquireOutput();
      out.write(bytes, offset, length);
      // LogGrep expects a line terminator for each record
      out.write('\n');
      batchLines++;

      if (trace != null) {
        // Does not produce tokens
        trace.trackIngestedLine(sourceId, 0);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private OutputStream acquireOutput() throws IOException {
    if (outputStream == null) {
      outputStream = createBatchOutput();
      batchLines = 0;
    } else if (batchLines >= maxBatchSize) {
      flushCurrentBatch();
      // Create new batch
      outputStream = createBatchOutput();
      batchLines = 0;
    }
    return outputStream;
  }

  private void flushCurrentBatch() throws IOException {
    if (outputStream != null) {
      outputStream.flush();
      outputStream.close();
      compressedBatches.add(compressBatch());
      Files.deleteIfExists(batchFile);
    }
  }

  private OutputStream createBatchOutput() throws IOException {
    Files.createDirectories(tempDir);
    return new BufferedOutputStream(new FileOutputStream(batchFile.toFile()));
  }

  @Override
  public void finish(FinishTrace trace) {
    try {
      final long start = System.nanoTime();
      flushCurrentBatch();
      trace.trackDataFinishTime(System.nanoTime() - start);
      trace.trackDataDiskUsage(directorySize(compressedDir));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void queryToken(byte[] utf8Token, LogConsumer logConsumer, QueryTrace trace, boolean loadData) {
    queryDirectory(queryBinary, compressedDir, utf8Token, lineBuffer, logConsumer);
  }

  @Override
  public void queryContains(byte[] utf8String, LogConsumer logConsumer, QueryTrace trace, boolean loadData) {
    queryDirectory(queryBinary, compressedDir, utf8String, lineBuffer, logConsumer);
  }

  @Override
  public long estimatedMemoryUsageBytes() {
    return 0;
  }

  @Override
  public void close() {
    // Nothing to do
  }

  private Path compressBatch() {
    final int batch = compressedBatches.size();
    final Path compressedBatch = compressedDir.resolve("batch_" + batch);

    try {
      Files.createDirectories(compressedDir);
      final Process process = new ProcessBuilder(
          compressorBinary, "-I", getPath(batchFile), "-O", getPath(compressedBatch), "-P", "T", "-Z", "zip")
          .start()
          .onExit()
          .get();

      if (process.exitValue() != 0) {
        throw new LogGrepException(String.format(
            "Compression failed with exit code %s for batch %s.", process.exitValue(), batchFile));
      }

      return compressedBatch;
    } catch (IOException | ExecutionException e) {
      throw new LogGrepException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new LogGrepException(e);
    }
  }

  private String getPath(Path file) {
    return file.toString();
  }
}
