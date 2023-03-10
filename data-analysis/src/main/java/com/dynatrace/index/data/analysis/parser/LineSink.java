package com.dynatrace.index.data.analysis.parser;

public interface LineSink {

  void acceptLine(byte[] utf8Bytes, int offset, int length, int posting);
}
