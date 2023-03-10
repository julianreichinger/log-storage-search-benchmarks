package com.dynatrace.index.storage;

/**
 * Used for the storage and compression of log data.
 */
public interface BatchWriter extends BatchReader {

  /**
   * Store a log line within the specified batch.
   */
  void addLogLine(byte[] bytes, int offset, int length, int batch);

  /**
   * Flush all data and construct the final file structure.
   */
  void flush();
}
