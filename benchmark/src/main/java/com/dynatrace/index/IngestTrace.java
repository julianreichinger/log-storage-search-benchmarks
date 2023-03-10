package com.dynatrace.index;

public interface IngestTrace {

  void trackIngestedLine(int sourceId, int tokens);
}
