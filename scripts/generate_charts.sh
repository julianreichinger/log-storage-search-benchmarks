#!/bin/bash -e

# Create chart directory
mkdir -p charts

cd visualizations

# Ingest speed chart
pipenv run python3.11 benchmark_graphs.py -i ../results/ingest -f "benchmark=com.dynatrace.index.benchmark.IngestBenchmark.ingest" -g "params/logFileName=1M_generated,5M_generated" -b "params/storeType=scan,csc,lucene,loggrep" -s "primaryMetric/rawData=ingest,secondaryMetrics/sketchFinishTimeSeconds/rawData=sketch_finish,secondaryMetrics/dataFinishTimeSeconds/rawData=data_finish" -u "ingest time [s]" -o ../charts/ingest_speed.svg

# Data usage chart
pipenv run python3.11 benchmark_graphs.py -i ../results/ingest -f "benchmark=com.dynatrace.index.benchmark.IngestBenchmark.ingest" -g "params/logFileName=1M_generated,5M_generated" -b "params/storeType=scan,csc,lucene,loggrep" -s "secondaryMetrics/dataDiskUsage/rawData=data,secondaryMetrics/sketchDiskUsage/rawData=sketch" -u "disk usage [MB]" -o ../charts/disk_usage.svg

# ID contains queries
pipenv run python3.11 benchmark_graphs.py -i ../results/query/needle_id_contains -f "benchmark=com.dynatrace.index.benchmark.QueryBenchmark.unknownIdContainsQuery" -g "params/logFileName=1M_generated,5M_generated" -b "params/storeType=scan,csc,lucene" -s "primaryMetric/rawData=throughput" -u "query throughput [1 / s]" -y "log" -o ../charts/id_contains_query.svg

# IP term queries
pipenv run python3.11 benchmark_graphs.py -i ../results/query/needle_ip -f "benchmark=com.dynatrace.index.benchmark.QueryBenchmark.unknownIpQuery" -g "params/logFileName=1M_generated,5M_generated" -b "params/storeType=scan,csc,lucene" -s "primaryMetric/rawData=throughput" -u "query throughput [1 / s]" -y "log" -o ../charts/ip_term_query.svg
