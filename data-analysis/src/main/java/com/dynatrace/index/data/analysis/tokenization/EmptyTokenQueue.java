package com.dynatrace.index.data.analysis.tokenization;

import com.dynatrace.index.data.analysis.tokenization.Tokenizer.TokenConsumer;
import java.util.List;

/**
 * Empty queue which can be used if no {@link DerivedTokenizer}s are specified.
 */
final class EmptyTokenQueue implements TokenQueue {

  private static final EmptyTokenQueue INSTANCE = new EmptyTokenQueue();

  private EmptyTokenQueue() {
    // Singleton
  }

  static EmptyTokenQueue getInstance() {
    return INSTANCE;
  }

  @Override
  public void clear() {
    // Do nothing
  }

  @Override
  public void push(TokenType type, int offset, int length) {
    // Do nothing
  }

  @Override
  public boolean matchesTypes(int[] expected) {
    return false;
  }

  @Override
  public int getOffset(int tailIndex) {
    throw newIllegalAccess();
  }

  @Override
  public int getLength(int tailIndex) {
    throw newIllegalAccess();
  }

  @Override
  public int getTokenTypeId(int tailIndex) {
    throw newIllegalAccess();
  }

  @Override
  public void processQueue(byte[] utf8Bytes, List<DerivedTokenizer> derivedTokenizers, TokenConsumer consumer) {
    // Do nothing
  }

  private IndexOutOfBoundsException newIllegalAccess() {
    return new IndexOutOfBoundsException("Tried to access empty token queue");
  }
}
