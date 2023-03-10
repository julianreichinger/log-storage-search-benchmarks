package com.dynatrace.index.storage;

/**
 * Used for reading log data stored by the {@link BatchWriter}.
 */
public interface BatchReader {

  /**
   * Read all log lines within the specified batch.
   */
  void readBatch(int batch, LogConsumer consumer);

  /**
   * @return the highest used batch number.
   */
  int getMaxBatch();

  void close();
}
