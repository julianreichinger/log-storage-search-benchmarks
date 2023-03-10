package com.dynatrace.index.data.analysis.tokenization;

import com.dynatrace.index.data.analysis.tokenization.Tokenizer.TokenConsumer;
import java.util.Arrays;
import java.util.List;

/**
 * Uses buffers which greatly exceed the sliding window size to avoid frequent copy operations.
 * The sliding window size is currently hard-coded to 5.
 */
final class BufferTokenQueue implements TokenQueue {

  private static final int SEQUENCE_LENGTH = 5;
  private static final int BUFFER_LENGTH = 1024;

  private final int[] typeQueue;
  private final int[] offsetQueue;
  private final int[] lengthQueue;

  private int pos;

  BufferTokenQueue() {
    this.typeQueue = new int[BUFFER_LENGTH];
    this.offsetQueue = new int[BUFFER_LENGTH];
    this.lengthQueue = new int[BUFFER_LENGTH];
    pos = SEQUENCE_LENGTH;
  }

  @Override
  public void clear() {
    Arrays.fill(typeQueue, 0, SEQUENCE_LENGTH, 0);
    Arrays.fill(offsetQueue, 0, SEQUENCE_LENGTH, 0);
    Arrays.fill(lengthQueue, 0, SEQUENCE_LENGTH, 0);
    pos = SEQUENCE_LENGTH;
  }

  @Override
  public void push(TokenType type, int offset, int length) {
    if (pos == BUFFER_LENGTH) {
      // Evacuate sequence to beginning of buffer
      System.arraycopy(typeQueue, BUFFER_LENGTH - SEQUENCE_LENGTH - 1, typeQueue, 0, SEQUENCE_LENGTH - 1);
      System.arraycopy(offsetQueue, BUFFER_LENGTH - SEQUENCE_LENGTH - 1, offsetQueue, 0, SEQUENCE_LENGTH - 1);
      System.arraycopy(lengthQueue, BUFFER_LENGTH - SEQUENCE_LENGTH - 1, lengthQueue, 0, SEQUENCE_LENGTH - 1);
      pos = SEQUENCE_LENGTH - 1;
    }

    typeQueue[pos] = type.getId();
    offsetQueue[pos] = offset;
    lengthQueue[pos] = length;
    pos++;
  }

  @Override
  public boolean matchesTypes(int[] expected) {
    int start = pos - expected.length;
    return Arrays.equals(typeQueue, start, pos, expected, 0, expected.length);
  }

  @Override
  public int getOffset(int tailIndex) {
    return offsetQueue[pos - tailIndex - 1];
  }

  @Override
  public int getLength(int tailIndex) {
    return lengthQueue[pos - tailIndex - 1];
  }

  @Override
  public int getTokenTypeId(int tailIndex) {
    return typeQueue[pos - tailIndex - 1];
  }

  @SuppressWarnings("ForLoopReplaceableByForEach") // avoid iterator creation
  @Override
  public void processQueue(byte[] utf8Bytes, List<DerivedTokenizer> derivedTokenizers, TokenConsumer consumer) {
    for (int i = 0; i < derivedTokenizers.size(); i++) {
      derivedTokenizers.get(i).processQueue(utf8Bytes, this, consumer);
    }
  }
}
