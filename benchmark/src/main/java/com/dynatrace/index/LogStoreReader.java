package com.dynatrace.index;

import com.dynatrace.index.storage.LogConsumer;

/**
 * Capable of searching and reading log lines stored in a log store.
 */
public interface LogStoreReader {

  /**
   * Search for all log lines containing the full, queried token.
   *
   * @param utf8Token token to query
   * @param logConsumer will be called with every matching log line
   * @param trace monitoring trace
   * @param loadData set to false in order to only check the speed of the index lookup (if applicable)
   */
  void queryToken(byte[] utf8Token, LogConsumer logConsumer, QueryTrace trace, boolean loadData);

  /**
   * Search for all log lines containing the queried string anywhere. This is a full sub-string search.
   *
   * @param utf8String string to query
   * @param logConsumer will be called with every matching log line
   * @param trace monitoring trace
   * @param loadData set to false in order to only check the speed of the index lookup (if applicable)
   */
  void queryContains(byte[] utf8String, LogConsumer logConsumer, QueryTrace trace, boolean loadData);

  /**
   * @return estimated memory usage of ONLY the internal indexing structure (if applicable)
   */
  long estimatedMemoryUsageBytes();

  void close();
}
