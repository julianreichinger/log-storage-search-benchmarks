package com.dynatrace.index.data.analysis.parser;

import com.dynatrace.index.data.analysis.tokenization.Tokenizer.TokenConsumer;

public interface TokenSink extends TokenConsumer {

  void startLine(byte[] utf8Bytes, int posting);

  default void endLine() {
    // Do nothing
  }
}
