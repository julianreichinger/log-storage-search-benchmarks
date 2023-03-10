package com.dynatrace.index;

public interface QueryTrace {

  void trackErrorRate(int falsePositives, int truePositives, int batches);
}
