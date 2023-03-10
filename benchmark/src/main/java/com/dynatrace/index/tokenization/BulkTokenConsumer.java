package com.dynatrace.index.tokenization;

/**
 * Accepts a collection of parsed tokens at once.
 */
public interface BulkTokenConsumer {

  void acceptTokens(byte[] bytes, int[] offsets, int[] lengths, int tokenCount, int posting);
}
