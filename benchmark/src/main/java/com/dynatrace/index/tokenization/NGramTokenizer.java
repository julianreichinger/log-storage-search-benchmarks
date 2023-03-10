package com.dynatrace.index.tokenization;

import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.ASCII_ALPHA_NUM;
import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.ASCII_ALPHA_NUM_TRI_GRAM;
import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.ASCII_OTHER;
import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.ASCII_OTHER_NGRAM;
import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.UNICODE;
import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.UNICODE_TWO_GRAM;

import com.dynatrace.index.data.analysis.tokenization.TokenQueue;
import com.dynatrace.index.data.analysis.tokenization.Tokenizer;
import com.dynatrace.index.data.analysis.tokenization.Tokenizer.TokenConsumer;
import com.dynatrace.index.data.analysis.tokenization.Tokenizers;

/**
 * Helper class to tokenize tokens into n-grams which are compatible with the indexed n-grams produced by the
 * {@link QueryTokenSink}.
 */
public final class NGramTokenizer {

  private NGramTokenizer() {
    // static helper
  }

  public static Tokenizer create() {
    return Tokenizers.builder()
        .addDerivedTokenizer(NGramTokenizer::asciiAlphaNumericTriGrams)
        .addDerivedTokenizer(NGramTokenizer::asciiOtherNGram)
        .addDerivedTokenizer(NGramTokenizer::unicodeTwoGrams)
        .build();
  }

  @SuppressWarnings("unused") // fulfill signature of DerivedTokenizer
  private static void asciiAlphaNumericTriGrams(byte[] utf8Bytes, TokenQueue tokenQueue, TokenConsumer consumer) {
    final int tokenTypeId = tokenQueue.getTokenTypeId(0);
    if (tokenTypeId == ASCII_ALPHA_NUM.getId()) {
      final int offset = tokenQueue.getOffset(0);
      final int length = tokenQueue.getLength(0);

      for (int i = 0; i < length - 2; i++) {
        consumer.accept(ASCII_ALPHA_NUM_TRI_GRAM, offset + i, 3);
      }
    }
  }

  private static void unicodeTwoGrams(byte[] utf8Bytes, TokenQueue tokenQueue, TokenConsumer consumer) {
    final int tokenTypeId = tokenQueue.getTokenTypeId(0);
    if (tokenTypeId == UNICODE.getId()) {
      final int offset = tokenQueue.getOffset(0);
      final int length = tokenQueue.getLength(0);

      for (int i = 0; i < length - 1; i++) {
        consumer.accept(UNICODE_TWO_GRAM, offset + i, 2);
      }
    }
  }

  private static void asciiOtherNGram(byte[] utf8Bytes, TokenQueue tokenQueue, TokenConsumer consumer) {
    final int tokenTypeId = tokenQueue.getTokenTypeId(0);

    if (tokenTypeId == ASCII_OTHER.getId()) {
      int offset = tokenQueue.getOffset(0);
      int length = tokenQueue.getLength(0);

      for (int i = 0; i < length; i++) {
        consumer.accept(ASCII_OTHER_NGRAM, offset + i, 1);
      }

      for (int i = 0; i < length - 1; i++) {
        consumer.accept(ASCII_OTHER_NGRAM, offset + i, 2);
      }

      for (int i = 0; i < length - 2; i++) {
        consumer.accept(ASCII_OTHER_NGRAM, offset + i, 3);
      }
    }
  }
}
