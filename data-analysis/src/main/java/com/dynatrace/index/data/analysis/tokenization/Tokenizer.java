package com.dynatrace.index.data.analysis.tokenization;

import com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType;

/**
 * Splits textual data into tokens and passes the tokens to a consumer.
 */
public interface Tokenizer {

  default void tokenize(byte[] utf8Bytes, TokenConsumer consumer) {
    tokenize(utf8Bytes, 0, utf8Bytes.length, consumer);
  }

  /**
   * Split the UTF-8 encoded text into tokens.
   *
   * @param utf8Bytes the UTF-8 data
   * @param offset the offset where to start parsing the data
   * @param length the number of bytes to parse within the array
   * @param consumer the consumer which accepts the found tokens
   */
  void tokenize(byte[] utf8Bytes, int offset, int length, TokenConsumer consumer);

  interface TokenConsumer {

    /**
     * Will be called for every found token.
     *
     * @param tokenType the type of the token
     * @param offset the offset of the token within the UTF-8 byte array
     * @param length the length of the token in bytes
     */
    void accept(TokenType tokenType, int offset, int length);
  }
}
