package com.dynatrace.index.data.analysis.tokenization;

import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.ASCII_ALPHA_NUM;
import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.ASCII_OTHER;
import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.UNICODE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType;
import com.dynatrace.index.data.analysis.tokenization.Tokenizer.TokenConsumer;
import java.util.ArrayList;
import java.util.List;

public final class Tokenizers {

  private Tokenizers() {
    // Static helpers
  }

  private static final int[] COMBO_TERM_SIGNATURE = {
      ASCII_ALPHA_NUM.getId(), ASCII_OTHER.getId(), ASCII_ALPHA_NUM.getId()
  };
  private static final int[] DOT_COMBO_TERM_SIGNATURE = {
      ASCII_ALPHA_NUM.getId(),
      ASCII_OTHER.getId(),
      ASCII_ALPHA_NUM.getId(),
      ASCII_OTHER.getId(),
      ASCII_ALPHA_NUM.getId()
  };

  private static final boolean[] COMBO_TERM_SEPARATOR = new boolean[256];

  static {
    COMBO_TERM_SEPARATOR['.'] = true;
    COMBO_TERM_SEPARATOR[':'] = true;
    COMBO_TERM_SEPARATOR['-'] = true;
    COMBO_TERM_SEPARATOR['_'] = true;
    COMBO_TERM_SEPARATOR['/'] = true;
    COMBO_TERM_SEPARATOR['\\'] = true;
    COMBO_TERM_SEPARATOR['@'] = true;
  }

  public static Tokenizer createFull() {
    return new TokenizerImpl(
        TokenQueue.createDefault(),
        List.of(ASCII_ALPHA_NUM, ASCII_OTHER, UNICODE),
        List.of(
            Tokenizers::comboTerms,
            Tokenizers::dotComboTerms,
            Tokenizers::asciiAlphaNumericTriGrams,
            Tokenizers::asciiOtherNGram,
            Tokenizers::unicodeTwoGrams
        ));
  }

  public static TokenizerBuilder builder() {
    return new TokenizerBuilder();
  }

  /**
   * Produces tokens of the form [ascii_alpha_num]+[.:-_/\@]{1}[ascii_alpha_num]+ with type {@link TokenType#COMBO}.
   * E.g. "host:93"
   */
  public static void comboTerms(byte[] utf8Bytes, TokenQueue tokenQueue, TokenConsumer consumer) {
    if (tokenQueue.matchesTypes(COMBO_TERM_SIGNATURE)) {
      final int separatorOffset = tokenQueue.getOffset(1);
      final int separatorLength = tokenQueue.getLength(1);

      if (separatorLength != 1) {
        return;
      }

      final byte separator = utf8Bytes[separatorOffset];
      if (COMBO_TERM_SEPARATOR[separator]) {
        final int offset = tokenQueue.getOffset(2);
        final int length = tokenQueue.getLength(2)
            + tokenQueue.getLength(1)
            + tokenQueue.getLength(0);
        consumer.accept(TokenType.COMBO, offset, length);
      }
    }
  }

  /**
   * Produces tokens of the form [ascii_alpha_num]+\.[ascii_alpha_num]+\.[ascii_alpha_num]
   * with type {@link TokenType#DOT_COMBO}.
   * E.g. "www.dynatrace.com"
   */
  public static void dotComboTerms(byte[] utf8Bytes, TokenQueue tokenQueue, TokenConsumer consumer) {
    if (tokenQueue.matchesTypes(DOT_COMBO_TERM_SIGNATURE)) {
      final int separatorOffset1 = tokenQueue.getOffset(1);
      final int separatorLength1 = tokenQueue.getLength(1);
      if (separatorLength1 != 1) {
        return;
      }
      final byte separator1 = utf8Bytes[separatorOffset1];
      if (separator1 != '.') {
        return;
      }

      final int separatorOffset2 = tokenQueue.getOffset(3);
      final int separatorLength2 = tokenQueue.getLength(3);
      if (separatorLength2 != 1) {
        return;
      }
      final byte separator2 = utf8Bytes[separatorOffset2];
      if (separator2 != '.') {
        return;
      }

      final int offset = tokenQueue.getOffset(4);
      final int length = (tokenQueue.getOffset(0) - offset) + tokenQueue.getLength(0);
      consumer.accept(TokenType.DOT_COMBO, offset, length);
    }
  }

  /**
   * Produces 3-grams from each token of type {@link TokenType#ASCII_ALPHA_NUM}
   * with type {@link TokenType#ASCII_ALPHA_NUM_TRI_GRAM}.
   */
  @SuppressWarnings("unused") // fulfill signature of DerivedTokenizer
  public static void asciiAlphaNumericTriGrams(byte[] utf8Bytes, TokenQueue tokenQueue, TokenConsumer consumer) {
    final int tokenTypeId = tokenQueue.getTokenTypeId(0);
    if (tokenTypeId == ASCII_ALPHA_NUM.getId()) {
      final int offset = tokenQueue.getOffset(0);
      final int length = tokenQueue.getLength(0);

      if (length <= 3) {
        return;
      }

      for (int i = 0; i < length - 2; i++) {
        consumer.accept(TokenType.ASCII_ALPHA_NUM_TRI_GRAM, offset + i, 3);
      }
    }
  }

  /**
   * Produces 2-grams from each token of type {@link TokenType#UNICODE} with type {@link TokenType#UNICODE_TWO_GRAM}.
   */
  public static void unicodeTwoGrams(byte[] utf8Bytes, TokenQueue tokenQueue, TokenConsumer consumer) {
    final int tokenTypeId = tokenQueue.getTokenTypeId(0);
    if (tokenTypeId == UNICODE.getId()) {
      final int offset = tokenQueue.getOffset(0);
      final int length = tokenQueue.getLength(0);

      if (length <= 2) {
        return;
      }

      for (int i = 0; i < length - 1; i++) {
        consumer.accept(TokenType.UNICODE_TWO_GRAM, offset + i, 2);
      }
    }
  }

  /**
   * Produces 1-grams, 2-grams and 3-grams from each token of type {@link TokenType#ASCII_OTHER}
   * with type {@link TokenType#ASCII_OTHER_NGRAM}.
   */
  public static void asciiOtherNGram(byte[] utf8Bytes, TokenQueue tokenQueue, TokenConsumer consumer) {
    final int tokenTypeId = tokenQueue.getTokenTypeId(0);
    if (tokenTypeId == ASCII_OTHER.getId()) {
      final int offset = tokenQueue.getOffset(0);
      final int length = tokenQueue.getLength(0);

      if (length == 1) {
        return;
      }

      for (int i = 0; i < length; i++) {
        consumer.accept(TokenType.ASCII_OTHER_NGRAM, offset + i, 1);
      }

      if (length == 2) {
        return;
      }

      for (int i = 0; i < length - 1; i++) {
        consumer.accept(TokenType.ASCII_OTHER_NGRAM, offset + i, 2);
      }

      if (length == 3) {
        return;
      }

      for (int i = 0; i < length - 2; i++) {
        consumer.accept(TokenType.ASCII_OTHER_NGRAM, offset + i, 3);
      }
    }
  }

  public static final class TokenizerBuilder {

    private final List<DerivedTokenizer> derivedTokenizers;
    private final List<TokenType> forwardedBaseTypes;

    private TokenizerBuilder() {
      derivedTokenizers = new ArrayList<>();
      forwardedBaseTypes = new ArrayList<>();
    }

    /**
     * All base token types ({@link TokenType#ASCII_ALPHA_NUM}, {@link TokenType#ASCII_OTHER},
     * {@link TokenType#UNICODE}) will be passed to the {@link TokenConsumer}.
     */
    public TokenizerBuilder forwardAllBaseTypes() {
      forwardedBaseTypes.add(ASCII_ALPHA_NUM);
      forwardedBaseTypes.add(ASCII_OTHER);
      forwardedBaseTypes.add(UNICODE);
      return this;
    }

    /**
     * The configured base {@link TokenType} will be passed to the {@link TokenConsumer}.
     */
    public TokenizerBuilder addForwardedBaseType(TokenType tokenType) {
      checkArgument(tokenType.getId() > 0 && tokenType.getId() <= 3, "Token type %s is not a base type", tokenType);
      forwardedBaseTypes.add(tokenType);
      return this;
    }

    /**
     * Add a derived tokenizer which will produce tokens based on the found base tokens.
     */
    public TokenizerBuilder addDerivedTokenizer(DerivedTokenizer derivedTokenizer) {
      derivedTokenizers.add(requireNonNull(derivedTokenizer));
      return this;
    }

    public Tokenizer build() {
      TokenQueue tokenQueue = derivedTokenizers.isEmpty()
          ? TokenQueue.createEmpty()
          : TokenQueue.createDefault();
      return new TokenizerImpl(tokenQueue, forwardedBaseTypes, derivedTokenizers);
    }
  }
}
