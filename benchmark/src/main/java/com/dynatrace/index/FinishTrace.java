package com.dynatrace.index;

public interface FinishTrace {

  void trackSketchFinishTime(long nanoseconds);

  void trackDataFinishTime(long nanoseconds);

  void trackSketchDiskUsage(long indexDiskBytes);

  void trackDataDiskUsage(long dataDiskBytes);

  void trackSketchMemoryUsage(long memoryBytes);
}
