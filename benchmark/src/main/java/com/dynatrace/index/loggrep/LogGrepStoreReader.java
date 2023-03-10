package com.dynatrace.index.loggrep;

import static com.dynatrace.index.loggrep.LogGrepStore.COMPRESSED_DIR;
import static com.dynatrace.index.storage.StorageDirectories.dataDirectory;
import static java.lang.String.format;

import com.dynatrace.index.LogStoreReader;
import com.dynatrace.index.QueryTrace;
import com.dynatrace.index.storage.LogConsumer;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

/**
 * LogStoreReader implementation using the <a href="https://dl.acm.org/doi/10.1145/3552326.3567484">LogGrep tool</a>
 * developed by Junyu Wei et.al.
 */
public class LogGrepStoreReader implements LogStoreReader {

  static final String QUERY_BINARY = "thulr_cmdline";

  private final String queryBinary;
  private final Path compressedDir;
  private final byte[] lineBuffer;

  private LogGrepStoreReader(Path binaryDir, Path compressedDir) {
    this.queryBinary = binaryDir.resolve(QUERY_BINARY).toString();
    this.compressedDir = compressedDir;
    // Make the query line buffer large enough to hold every possible log line for simplicity
    this.lineBuffer = new byte[64 * 1024];
  }

  public static LogGrepStoreReader loadFromDisk(Path binaryDir, Path storageDirectory) {
    final Path dataDir = dataDirectory(storageDirectory);
    final Path compressedDir = dataDir.resolve(COMPRESSED_DIR);

    return new LogGrepStoreReader(binaryDir, compressedDir);
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

  static void queryDirectory(
      String queryBinary, Path directory, byte[] queryToken, byte[] buffer, LogConsumer logConsumer) {

    try {
      final String queryString = new String(queryToken, StandardCharsets.UTF_8);
      final Process process = new ProcessBuilder(queryBinary, directory.toString(), queryString)
          .start();

      final InputStream inputStream = process.getInputStream();

      // Results are transferred via piping
      int length = inputStream.read(buffer);
      while (length > 0) {
        int offset = 0;
        // We will always find at least one line break in the benchmarks within 64KB
        int lineBreak = indexOf(buffer, offset, length, (byte) '\n');
        while (lineBreak > 0) {
          logConsumer.acceptLog(buffer, offset, lineBreak - offset);

          offset = lineBreak + 1;
          lineBreak = indexOf(buffer, offset, length, (byte) '\n');
        }

        if (offset < length) {
          // Just hand out the cut off last log-line as a separate line -> good enough for benchmark, as this anyway
          // only consumed in a black hole
          logConsumer.acceptLog(buffer, offset, length - offset);
        }

        length = inputStream.read(buffer);
      }

      process.onExit().get();
      if (process.exitValue() != 0) {
        throw new LogGrepException(format(
            "Query failed for token %s with exit value %s.", queryString, process.exitValue()));
      }
    } catch (IOException | ExecutionException e) {
      throw new LogGrepException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static int indexOf(byte[] buffer, int startOffset, int endOffset, byte character) {
    for (int index = startOffset; index < endOffset; index++) {
      if (buffer[index] == character) {
        return index;
      }
    }

    return -1;
  }
}
