#!/bin/bash -e

# build the benchmark executable
./gradlew :benchmark:clean :benchmark:jmhJar --no-build-cache

# INGEST BENCHMARKS
mkdir -p results/ingest

java -jar benchmark/build/libs/benchmarks.jar "IngestBenchmark" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=csc -p cscSizeMB=8 -p tokenizer=full -rf json -rff results/ingest/1M_generated_csc.json
java -jar benchmark/build/libs/benchmarks.jar "IngestBenchmark" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=csc-bf -p cscSizeMB=8 -p tokenizer=full -rf json -rff results/ingest/1M_generated_cscbf.json
java -jar benchmark/build/libs/benchmarks.jar "IngestBenchmark" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=lucene -p tokenizer=combo -rf json -rff results/ingest/1M_generated_lucene.json
java -jar benchmark/build/libs/benchmarks.jar "IngestBenchmark" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=scan -rf json -rff results/ingest/1M_generated_scan.json
java -jar benchmark/build/libs/benchmarks.jar "IngestBenchmark" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=loggrep -rf json -rff results/ingest/1M_generated_loggrep.json
rm -r tmp/*

java -jar benchmark/build/libs/benchmarks.jar "IngestBenchmark" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=csc -p cscSizeMB=32 -p tokenizer=full -p maxBatchCount=4096 -rf json -rff results/ingest/5M_generated_csc.json
java -jar benchmark/build/libs/benchmarks.jar "IngestBenchmark" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=csc-bf -p cscSizeMB=32 -p tokenizer=full -p maxBatchCount=4096 -rf json -rff results/ingest/5M_generated_cscbf.json
java -jar benchmark/build/libs/benchmarks.jar "IngestBenchmark" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=lucene -p tokenizer=combo -p maxBatchCount=4096 -rf json -rff results/ingest/5M_generated_lucene.json
java -jar benchmark/build/libs/benchmarks.jar "IngestBenchmark" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=scan -p maxBatchCount=4096 -rf json -rff results/ingest/5M_generated_scan.json
java -jar benchmark/build/libs/benchmarks.jar "IngestBenchmark" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=loggrep -p maxBatchCount=4096 -rf json -rff results/ingest/5M_generated_loggrep.json
rm -r tmp/*

# QUERY BENCHMARKS

############# ID QUERIES #############

# "Needle in the haystack" ID queries
mkdir -p results/query/needle_id

java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIdQuery" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=csc -p cscSizeMB=8 -p tokenizer=full -p queryMode=COLD -rf json -rff results/query/needle_id/1M_csc.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIdQuery" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=csc-bf -p cscSizeMB=8 -p tokenizer=full -p queryMode=COLD -rf json -rff results/query/needle_id/1M_cscbf.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIdQuery" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=lucene -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/needle_id/1M_lucene.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIdQuery" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=scan -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/needle_id/1M_scan.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIdQuery" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=loggrep -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/needle_id/1M_loggrep.json
rm -r tmp/*

java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIdQuery" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=csc -p cscSizeMB=32 -p tokenizer=full -p queryMode=COLD -p maxBatchCount=4096 -rf json -rff results/query/needle_id/5M_csc.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIdQuery" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=csc-bf -p cscSizeMB=32 -p tokenizer=full -p queryMode=COLD -p maxBatchCount=4096 -rf json -rff results/query/needle_id/5M_cscbf.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIdQuery" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=lucene -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/needle_id/5M_lucene.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIdQuery" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=scan -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/needle_id/5M_scan.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIdQuery" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=loggrep -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/needle_id/5M_loggrep.json
rm -r tmp/*

# "Needle in the haystack" ID contains query
mkdir -p results/query/needle_id_contains

java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIdContainsQuery" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=csc -p cscSizeMB=8 -p tokenizer=full -p queryMode=COLD -rf json -rff results/query/needle_id_contains/1M_csc.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIdContainsQuery" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=csc-bf -p cscSizeMB=8 -p tokenizer=full -p queryMode=COLD -rf json -rff results/query/needle_id_contains/1M_cscbf.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIdContainsQuery" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=lucene -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/needle_id_contains/1M_lucene.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIdContainsQuery" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=scan -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/needle_id_contains/1M_scan.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIdContainsQuery" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=loggrep -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/needle_id_contains/1M_loggrep.json
rm -r tmp/*

java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIdContainsQuery" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=csc -p cscSizeMB=32 -p tokenizer=full -p queryMode=COLD -p maxBatchCount=4096 -rf json -rff results/query/needle_id_contains/5M_csc.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIdContainsQuery" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=csc-bf -p cscSizeMB=32 -p tokenizer=full -p queryMode=COLD -p maxBatchCount=4096 -rf json -rff results/query/needle_id_contains/5M_cscbf.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIdContainsQuery" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=lucene -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/needle_id_contains/5M_lucene.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIdContainsQuery" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=scan -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/needle_id_contains/5M_scan.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIdContainsQuery" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=loggrep -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/needle_id_contains/5M_loggrep.json
rm -r tmp/*

############# IP QUERIES #############

# "Needle in the haystack" IP queries
mkdir -p results/query/needle_ip

java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIpQuery" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=csc -p cscSizeMB=8 -p tokenizer=full -p queryMode=COLD -rf json -rff results/query/needle_ip/1M_csc.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIpQuery" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=csc-bf -p cscSizeMB=8 -p tokenizer=full -p queryMode=COLD -rf json -rff results/query/needle_ip/1M_cscbf.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIpQuery" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=lucene -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/needle_ip/1M_lucene.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIpQuery" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=scan -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/needle_ip/1M_scan.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIpQuery" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=loggrep -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/needle_ip/1M_loggrep.json
rm -r tmp/*

java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIpQuery" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=csc -p cscSizeMB=32 -p tokenizer=full -p queryMode=COLD -p maxBatchCount=4096 -rf json -rff results/query/needle_ip/5M_csc.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIpQuery" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=csc-bf -p cscSizeMB=32 -p tokenizer=full -p queryMode=COLD -p maxBatchCount=4096 -rf json -rff results/query/needle_ip/5M_cscbf.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIpQuery" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=lucene -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/needle_ip/5M_lucene.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIpQuery" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=scan -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/needle_ip/5M_scan.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIpQuery" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=loggrep -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/needle_ip/5M_loggrep.json
rm -r tmp/*

# "Needle in the haystack" IP contains query
mkdir -p results/query/needle_ip_contains

java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIpContainsQuery" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=csc -p cscSizeMB=8 -p tokenizer=full -p queryMode=COLD -rf json -rff results/query/needle_ip_contains/1M_csc.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIpContainsQuery" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=csc-bf -p cscSizeMB=8 -p tokenizer=full -p queryMode=COLD -rf json -rff results/query/needle_ip_contains/1M_cscbf.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIpContainsQuery" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=lucene -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/needle_ip_contains/1M_lucene.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIpContainsQuery" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=scan -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/needle_ip_contains/1M_scan.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIpContainsQuery" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=loggrep -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/needle_ip_contains/1M_loggrep.json
rm -r tmp/*

java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIpContainsQuery" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=csc -p cscSizeMB=32 -p tokenizer=full -p queryMode=COLD -p maxBatchCount=4096 -rf json -rff results/query/needle_ip_contains/5M_csc.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIpContainsQuery" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=csc-bf -p cscSizeMB=32 -p tokenizer=full -p queryMode=COLD -p maxBatchCount=4096 -rf json -rff results/query/needle_ip_contains/5M_cscbf.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIpContainsQuery" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=lucene -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/needle_ip_contains/5M_lucene.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIpContainsQuery" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=scan -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/needle_ip_contains/5M_scan.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.unknownIpContainsQuery" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=loggrep -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/needle_ip_contains/5M_loggrep.json
rm -r tmp/*

# Frequent data queries

mkdir -p results/query/frequent

java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.tokenQuery" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=csc -p cscSizeMB=8 -p tokenizer=full -p queryMode=COLD -rf json -rff results/query/frequent/1M_csc.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.tokenQuery" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=csc-bf -p cscSizeMB=8 -p tokenizer=full -p queryMode=COLD -rf json -rff results/query/frequent/1M_cscbf.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.tokenQuery" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=lucene -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/frequent/1M_lucene.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.tokenQuery" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=scan -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/frequent/1M_scan.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.tokenQuery" -p logFileName=data/1M_generated -p rootDirName=tmp -p storeType=loggrep -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/frequent/1M_loggrep.json
rm -r tmp/*

java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.tokenQuery" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=csc -p cscSizeMB=32 -p tokenizer=full -p queryMode=COLD -rf json -rff results/query/frequent/5M_csc.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.tokenQuery" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=csc-bf -p cscSizeMB=32 -p tokenizer=full -p queryMode=COLD -rf json -rff results/query/frequent/5M_cscbf.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.tokenQuery" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=lucene -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/frequent/5M_lucene.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.tokenQuery" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=scan -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/frequent/5M_scan.json
java -jar benchmark/build/libs/benchmarks.jar "QueryBenchmark.tokenQuery" -p logFileName=data/5M_generated -p rootDirName=tmp -p storeType=loggrep -p tokenizer=combo -p queryMode=COLD -rf json -rff results/query/frequent/5M_loggrep.json
rm -r tmp/*