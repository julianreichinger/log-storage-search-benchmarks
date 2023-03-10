package com.dynatrace.index.data.analysis.tokenization;

import com.dynatrace.index.data.analysis.tokenization.Tokenizer.TokenConsumer;
import java.util.Arrays;
import java.util.List;

/**
 * Tracks a sliding window of the latest parsed base tokens, so {@link DerivedTokenizer} can create combo terms.
 */
public interface TokenQueue {

  /**
   * Clear the queue.
   */
  void clear();

  /**
   * Track a new base token.
   */
  void push(TokenType type, int offset, int length);

  /**
   * Check if the latest tokens match the provided type signature.
   *
   * @param expected IDs of the wanted token types. Must not be longer than the supported sliding window size of
   *     the token queue implementation.
   */
  boolean matchesTypes(int[] expected);

  /**
   * Get the offset of the specified token.
   *
   * @param tailIndex index 0 accesses the latest token, index 1 the token before that, etc.
   */
  int getOffset(int tailIndex);

  /**
   * Get the length of the specified token.
   *
   * @param tailIndex index 0 accesses the latest token, index 1 the token before that, etc.
   */
  int getLength(int tailIndex);

  /**
   * Get the token type ID of the specified token.
   *
   * @param tailIndex index 0 accesses the latest token, index 1 the token before that, etc.
   */
  int getTokenTypeId(int tailIndex);

  /**
   * Pass the queue to each of the derived tokenizers.
   */
  void processQueue(byte[] utf8Bytes, List<DerivedTokenizer> derivedTokenizers, TokenConsumer consumer);

  /**
   * @return creates a token queue with a sliding window size of 0, which can be used if no derived tokenizers are
   *     configured
   */
  static TokenQueue createEmpty() {
    return EmptyTokenQueue.getInstance();
  }

  /**
   * @return creates a token queue with a sliding window size of 5
   */
  static TokenQueue createDefault() {
    return new BufferTokenQueue();
  }

  /**
   * Specifies the type of parsed tokens.
   */
  enum TokenType {

    UNDEFINED(0),

    // Base types
    /**
     * Contains only characters [a-zA-Z0-9]
     */
    ASCII_ALPHA_NUM(1),
    /**
     * Contains only ASCII characters except the ones defined in ASCII_ALPHA_NUM.
     */
    ASCII_OTHER(2),
    /**
     * Contains only non-ASCII characters.
     */
    UNICODE(3),

    // Derived types (combining / modifying base tokens)
    /**
     * See {@link Tokenizers#comboTerms(byte[], TokenQueue, TokenConsumer)}.
     */
    COMBO(4),
    /**
     * See {@link Tokenizers#dotComboTerms(byte[], TokenQueue, TokenConsumer)}.
     */
    DOT_COMBO(5),
    /**
     * See {@link Tokenizers#asciiAlphaNumericTriGrams(byte[], TokenQueue, TokenConsumer)}.
     */
    ASCII_ALPHA_NUM_TRI_GRAM(6),
    /**
     * See {@link Tokenizers#unicodeTwoGrams(byte[], TokenQueue, TokenConsumer)}.
     */
    UNICODE_TWO_GRAM(7),
    /**
     * See {@link Tokenizers#asciiOtherNGram(byte[], TokenQueue, TokenConsumer)}.
     */
    ASCII_OTHER_NGRAM(8),

    /**
     * Should be used for custom {@link DerivedTokenizer} implementations.
     */
    CUSTOM(9);

    private final int id;

    TokenType(int id) {
      this.id = id;
    }

    public int getId() {
      return id;
    }

    private static final TokenType[] TOKENS_BY_UTF = new TokenType[256];

    static {
      Arrays.fill(TOKENS_BY_UTF, 0, 128, ASCII_OTHER);
      Arrays.fill(TOKENS_BY_UTF, 128, 256, UNICODE);
      Arrays.fill(TOKENS_BY_UTF, 'a', 'z' + 1, ASCII_ALPHA_NUM);
      Arrays.fill(TOKENS_BY_UTF, 'A', 'Z' + 1, ASCII_ALPHA_NUM);
      Arrays.fill(TOKENS_BY_UTF, '0', '9' + 1, ASCII_ALPHA_NUM);
    }

    public static TokenType getType(byte utf8Byte) {
      return TOKENS_BY_UTF[utf8Byte & 0xff];
    }
  }
}
