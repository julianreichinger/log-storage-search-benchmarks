package com.dynatrace.index.data.analysis.parser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Parses a log file in the format described below. Allows to separate IO from processing.
 * <p>
 * Required file format:
 * <pre>
 * [integer],[arbitrary text]\n
 * [integer],[arbitrary text]\n
 * ...
 * </pre>
 */
public final class LogLineReader {

  public static final int DEFAULT_BATCH_SIZE = 32 * 1024 * 1024;

  private static final byte SEPARATOR = ',';
  private static final byte LINE_SEPARATOR = '\n';

  private final BatchBuffer batchBuffer;
  private final int maxLineLength;

  private int position;

  private LogLineReader(InputStream in, int batchSizeBytes, int maxLineLength) {
    this.batchBuffer = new BatchBuffer(in, batchSizeBytes);
    this.maxLineLength = maxLineLength;
  }

  public static LogLineReader create(InputStream in, int batchSizeBytes, int maxLineLength) {
    return new LogLineReader(in, batchSizeBytes, maxLineLength);
  }

  public static void parseStream(InputStream in, LineSink lineSink, int maxLineLength) throws IOException {
    LogLineReader parser = create(in, DEFAULT_BATCH_SIZE, maxLineLength);
    while (parser.readBatch()) {
      parser.parseBatch(lineSink);
    }
  }

  public static void parseFile(Path logFile, LineSink lineSink, int maxLineLength)
      throws IOException {
    try (InputStream in = Files.newInputStream(logFile, StandardOpenOption.READ)) {
      parseStream(in, lineSink, maxLineLength);
    }
  }

  /**
   * Read the next data batch into memory.
   *
   * @return true if more data could be read from the input stream
   */
  public boolean readBatch() throws IOException {
    final int readBytes = batchBuffer.readBatch(position);
    position = 0;
    return readBytes > 0;
  }

  public void parseBatch(LineSink lineSink) {
    if (batchBuffer.length() == 0 || position >= batchBuffer.length()) {
      return;
    }

    int splitPosition = batchBuffer.indexOf(position, SEPARATOR);
    while (splitPosition > 0) {
      final int posting = Integer.parseInt(batchBuffer, position, splitPosition, 10);
      final int lineStart = splitPosition + 1;
      int lineEnd = findLineEnd(lineStart);

      if (lineEnd < 0) {
        break;
      }

      final int lineLength = Math.min(lineEnd - lineStart, maxLineLength);
      lineSink.acceptLine(batchBuffer.getBuffer(), lineStart, lineLength, posting);

      position = lineEnd + 1;
      splitPosition = batchBuffer.indexOf(position, SEPARATOR);
    }
  }

  private int findLineEnd(int lineStart) {
    final int lineBreak = batchBuffer.indexOf(lineStart, LINE_SEPARATOR);
    if (lineBreak < 0 && batchBuffer.reachedEof()) {
      return batchBuffer.length();
    }
    return lineBreak;
  }
}
