package com.dynatrace.index.data.analysis.parser;

import static com.dynatrace.index.data.analysis.parser.LogLineReader.DEFAULT_BATCH_SIZE;

import com.dynatrace.index.data.analysis.tokenization.Lowercase;
import com.dynatrace.index.data.analysis.tokenization.Tokenizer;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Parses and tokenizes a log file in the format described below. Allows to separate IO from processing.
 * <p>
 * Required file format:
 * <pre>
 * [integer],[arbitrary text]\n
 * [integer],[arbitrary text]\n
 * ...
 * </pre>
 */
public final class LogParser {

  private final LogLineReader lineReader;
  private byte[] lowercaseBuffer;

  private LogParser(LogLineReader lineReader) {
    this.lineReader = lineReader;
    this.lowercaseBuffer = new byte[64 * 1024];
  }

  public static LogParser create(InputStream in, int batchSizeBytes, int maxLineLength) {
    final LogLineReader lineReader = LogLineReader.create(in, batchSizeBytes, maxLineLength);
    return new LogParser(lineReader);
  }

  public static void parseStream(
      InputStream in, Tokenizer tokenizer, TokenSink tokenSink, int maxLineLength, boolean lowerCase)
      throws IOException {
    LogParser parser = create(in, DEFAULT_BATCH_SIZE, maxLineLength);
    while (parser.readBatch()) {
      parser.parseBatch(tokenizer, tokenSink, lowerCase);
    }
  }

  public static void parseFile(Path logFile, Tokenizer tokenizer, TokenSink tokenSink)
      throws IOException {
    try (InputStream in = Files.newInputStream(logFile, StandardOpenOption.READ)) {
      parseStream(in, tokenizer, tokenSink, Integer.MAX_VALUE, true);
    }
  }

  public static void parseFile(
      Path logFile, Tokenizer tokenizer, TokenSink tokenSink, int maxLineLength, boolean lowerCase)
      throws IOException {
    try (InputStream in = Files.newInputStream(logFile, StandardOpenOption.READ)) {
      parseStream(in, tokenizer, tokenSink, maxLineLength, lowerCase);
    }
  }

  /**
   * Read the next data batch into memory.
   *
   * @return true if more data could be read from the input stream
   */
  public boolean readBatch() throws IOException {
    return lineReader.readBatch();
  }

  /**
   * Parse the next data batch and pass each line to the tokenizer.
   *
   * @param tokenizer will be used to tokenize each line
   * @param tokenSink will be notified about new lines and tokens
   */
  public void parseBatch(Tokenizer tokenizer, TokenSink tokenSink, boolean lowerCase) {
    lineReader.parseBatch((utf8Bytes, offset, length, posting) -> {
      if (lowerCase) {
        utf8Bytes = toLowerCase(utf8Bytes, offset, length);
        offset = 0;
      }
      tokenSink.startLine(utf8Bytes, posting);
      tokenizer.tokenize(utf8Bytes, offset, length, tokenSink);
      tokenSink.endLine();
    });
  }

  private byte[] toLowerCase(byte[] bytes, int lineStart, int lineLength) {
    if (lowercaseBuffer.length < lineLength) {
      int newCapacity = Math.max(lineLength, lowercaseBuffer.length * 2);
      lowercaseBuffer = new byte[newCapacity];
    }
    Lowercase.toLowerCase(bytes, lineStart, lineLength, lowercaseBuffer);
    return lowercaseBuffer;
  }
}
