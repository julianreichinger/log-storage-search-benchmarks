package com.dynatrace.index.data.analysis.tokenization;

import static java.util.Objects.requireNonNull;

import com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType;
import java.util.List;

/**
 * Default tokenizer implementation. Data is first separated into the three base token types:
 * <ul>
 *   <li>{@link TokenType#ASCII_ALPHA_NUM}
 *   <li>{@link TokenType#ASCII_OTHER}
 *   <li>{@link TokenType#UNICODE}
 * </ul>
 *
 * Each found base token is passed to a {@link TokenQueue} and all configured {@link DerivedTokenizer}s are evaluated
 * against the {@link TokenQueue}.
 */
final class TokenizerImpl implements Tokenizer {

  private final TokenQueue tokenQueue;
  private final List<DerivedTokenizer> derivedTokenizers;
  private final boolean[] baseTypeForwarding;

  TokenizerImpl(
      TokenQueue tokenQueue,
      List<TokenType> baseTypes,
      List<DerivedTokenizer> derivedTokenizers) {
    this.tokenQueue = requireNonNull(tokenQueue);
    this.derivedTokenizers = requireNonNull(derivedTokenizers);

    this.baseTypeForwarding = new boolean[4];
    for (TokenType type : baseTypes) {
      baseTypeForwarding[type.getId()] = true;
    }
  }

  @Override
  public void tokenize(byte[] utf8Bytes, int offset, int length, TokenConsumer consumer) {
    tokenQueue.clear();

    int currentOffset = offset;
    TokenType currentType = null;
    int end = offset + length;
    int i = offset;
    for (; i < end; i++) {
      TokenType tokenType = TokenType.getType(utf8Bytes[i]);
      if (currentType == null) {
        currentType = tokenType;
      } else if (tokenType != currentType) {
        addToken(utf8Bytes, currentType, currentOffset, i, consumer);
        currentOffset = i;
        currentType = tokenType;
      }
    }

    if (i > currentOffset && currentType != null) {
      addToken(utf8Bytes, currentType, currentOffset, i, consumer);
    }
  }

  private void addToken(byte[] utf8Bytes, TokenType tokenType, int startOffset, int endOffset, TokenConsumer consumer) {
    int length = endOffset - startOffset;
    if (baseTypeForwarding[tokenType.getId()]) {
      consumer.accept(tokenType, startOffset, length);
    }
    tokenQueue.push(tokenType, startOffset, length);
    tokenQueue.processQueue(utf8Bytes, derivedTokenizers, consumer);
  }
}
