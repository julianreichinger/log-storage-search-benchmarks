package com.dynatrace.index.csc;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.IntConsumer;

public interface CscFilter {

  /**
   * Update multiple token/posting pairs in the filter.
   *
   * @param bytes backing byte array of the tokens
   * @param offsets offsets of each token
   * @param lengths lengths of each token
   * @param count number of tokens
   * @param posting posting defining where a token appeared
   */
  void update(byte[] bytes, int[] offsets, int[] lengths, int count, int posting);

  /**
   * Update a token/posting entry in the filter.
   *
   * @param bytes backing byte array of the token
   * @param offset offset of the token within the backing byte array
   * @param length length of the token
   * @param posting posting defining where a token appeared
   */
  void update(byte[] bytes, int offset, int length, int posting);

  /**
   * Retrieve all postings for a queried token.
   *
   * @param bytes the bytes of the queried token
   * @param postingsConsumer will be called for all postings of the token
   */
  void query(byte[] bytes, IntConsumer postingsConsumer);

  /**
   * Retrieve the postings shared by all queried tokens.
   *
   * @param bytes the bytes of the queried tokens
   * @param postingsConsumer will be called for the shared postings
   */
  void queryAll(byte[][] bytes, IntConsumer postingsConsumer);

  long estimatedMemoryUsageBytes();

  void close();

  void writeTo(OutputStream out) throws IOException;
}
