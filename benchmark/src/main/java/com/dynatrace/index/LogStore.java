package com.dynatrace.index;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * A log store must be capable of storing, searching and reading log lines.
 */
public interface LogStore extends LogStoreReader {

  default void addLogLine(byte[] bytes, int offset, int length, int sourceId) {
    addLogLine(bytes, offset, length, sourceId, null);
  }

  /**
   * Add a new log line to the store.
   *
   * @param bytes backing byte array holding the log line
   * @param offset start offset of the log line
   * @param length length of the log line
   * @param sourceId the "source" which produced the log line (e.g. an instance of an application)
   * @param ingestTrace monitoring trace
   */
  void addLogLine(byte[] bytes, int offset, int length, int sourceId, @Nullable IngestTrace ingestTrace);

  /**
   * Flush all data to disk and prepare the index for queries. After this method has been called, all data should
   * be visible for queries.
   */
  void finish(FinishTrace trace) throws IOException;
}
