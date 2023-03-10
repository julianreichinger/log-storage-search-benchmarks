package com.dynatrace.index.storage;

/**
 * Accepts individual log lines.
 */
public interface LogConsumer {

  void acceptLog(byte[] bytes, int offset, int length);
}
