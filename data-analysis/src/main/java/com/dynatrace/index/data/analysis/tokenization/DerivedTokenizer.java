package com.dynatrace.index.data.analysis.tokenization;

import com.dynatrace.index.data.analysis.tokenization.Tokenizer.TokenConsumer;

/**
 * Produces new tokens based on parsed base tokens.
 */
public interface DerivedTokenizer {

  /**
   * Derive new tokens based on the base tokens stored in the {@link TokenQueue}.
   *
   * @param utf8Bytes bytes which are tokenized
   * @param tokenQueue queue of the latest base types
   * @param consumer token consumer
   */
  void processQueue(byte[] utf8Bytes, TokenQueue tokenQueue, TokenConsumer consumer);
}
