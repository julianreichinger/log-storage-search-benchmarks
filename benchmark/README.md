# Benchmarks

## How to build the JMH jar

> ./gradlew :benchmark:clean :benchmark:jmhJar --no-build-cache

The built JAR is located under

> project_root/benchmark/build/libs/benchmarks.jar

## How to run a benchmark

When running a benchmark, the benchmark class and all its mandatory parameters have to be specified.
See classes "IngestState" for configurable parameters of the "IngestBenchmark" and "QueryState" 
for configurable parameters of the "QueryBenchmark".

> java -jar ./benchmark/build/libs/benchmarks.jar "IngestBenchmark" -p logFileName=./data/1M_generated

## Benchmark Classes

### IngestBenchmark

Tests the ingest performance of the different log store implementations and measures some secondary metrics.

**Result metrics:**
* primary metric: ingest time in seconds
* dataDiskUsage: disk usage in MB of the compressed log data
* sketchDiskUsage: disk usage in MB of the immutable index or sketch structure
* sketchMemoryUsage: memory usage in MB of the index or sketch when the log store is finished
* sketchFinishTimeSeconds: Time needed to build the immutable index or sketch structure and write it to disk
* dataFinishTimeSeconds: Time needed to compress and flush buffered log data when the log store is finished
* sourceCount: the number of log sources in the data set
* lineCount: the number of log lines in the data set
* tokenCount: total token count in the data set

**Parameters:**
* See class "IngestState" for configurable parameters

### QueryBenchmark

Tests the query performance of the different log store implementations and measures some secondary metrics.

**Result metrics:**
* primary metric: query throughput (operations per second)
* falsePositives: sum of false positives over all executed queries
* truePositives: sum of true positives over all executed queries
* batches: sum of searched batches over all executed queries (also includes true negatives)
* query count: number of executed queries
* memory usage: estimated memory usage in bytes of the log store reader

**Parameters:**
* See class "QueryState" for configurable parameters
