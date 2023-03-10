package com.dynatrace.index.tokenization;

import com.dynatrace.index.data.analysis.parser.TokenSink;
import com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType;
import java.util.Arrays;

/**
 * Gathers all tokens of a line and indexes them at once.
 */
public final class IngestTokenSink implements TokenSink {

  private final BulkTokenConsumer consumer;

  private int[] offsets;
  private int[] lengths;
  private int tokenCount;
  private byte[] utf8Bytes;
  private int posting;

  public IngestTokenSink(BulkTokenConsumer consumer) {
    this.consumer = consumer;
    this.offsets = new int[64 * 1024];
    this.lengths = new int[64 * 1024];
  }

  @Override
  public void startLine(byte[] utf8Bytes, int posting) {
    this.utf8Bytes = utf8Bytes;
    this.posting = posting;
  }

  @Override
  public void accept(TokenType tokenType, int offset, int length) {
    if (tokenCount > offsets.length) {
      offsets = Arrays.copyOf(offsets, offsets.length * 2);
      lengths = Arrays.copyOf(lengths, lengths.length * 2);
    }
    offsets[tokenCount] = offset;
    lengths[tokenCount] = length;
    tokenCount++;
  }

  @Override
  public void endLine() {
    consumer.acceptTokens(utf8Bytes, offsets, lengths, tokenCount, posting);
    tokenCount = 0;
  }

  public int getTokenCount() {
    return tokenCount;
  }
}
