package com.dynatrace.index.data.analysis.parser;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Helper class to parse an input stream in batches.
 */
@SuppressWarnings("NullableProblems")
public class BatchBuffer implements CharSequence {

  private final InputStream in;
  private final byte[] buffer;
  private int length;
  private boolean eof;

  public BatchBuffer(InputStream in, int capacity) {
    this.in = requireNonNull(in);
    this.buffer = new byte[capacity];
  }

  @Override
  public int length() {
    return length;
  }

  @Override
  public char charAt(int index) {
    return (char) buffer[index];
  }

  @Override
  public CharSequence subSequence(int start, int end) {
    return new String(Arrays.copyOfRange(buffer, start, end), StandardCharsets.UTF_8);
  }

  byte[] getBuffer() {
    return buffer;
  }

  public boolean reachedEof() {
    return eof;
  }

  public int readBatch(int remainderOffset) throws IOException {
    final int remainderLength = Math.max(0, length - remainderOffset);
    if (remainderLength > 0) {
      System.arraycopy(buffer, remainderOffset, buffer, 0, remainderLength);
    }
    int readBytes = in.readNBytes(buffer, remainderLength, buffer.length - remainderLength);
    length = readBytes + remainderLength;
    if (length < buffer.length) {
      eof = true;
    }
    return length;
  }

  public int indexOf(int startOffset, byte character) {
    for (int index = startOffset; index < length; index++) {
      if (buffer[index] == character) {
        return index;
      }
    }

    return -1;
  }
}
