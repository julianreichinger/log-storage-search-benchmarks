# Log Storage and Search Benchmarks

This repository contains the benchmarks, data generation and visualization code used in the paper 
"DynaWarp - Efficient, large-scale log storage and retrieval". 

While many of the things will also work on Windows, we only provide convenient scripts and guidance for Linux. 

## Generating test data

We provide utilities to generate production-like data sets from open-source log data. To work, the data generator first
needs a set of application logs. It will then randomly sample sequences of log lines from the different
applications and combine them into a single, heterogeneous data set. Each sampled sequence is treated as a separate
log source. To achieve a realistic composition of log sources, the sampling process follows a data distribution we
collected from production data sets.

**Required**
- OpenJDK >= 11
    - Paper benchmarks were performed with OpenJDK 17.0.7

**Steps**
- Download and extract the LogHub data sets into an `input` directory within the repository root dir
  - https://github.com/logpai/loghub 
    - The readme contains a link to the full data set
  - Each set of logs (e.g. Hadoop, Apache, ...) must reside in a separate subdirectory within the `input` directory
    - Each subdirectory will be recursively searched for files with a `.log` extension
- (optional) Prepare data to correctly handle multi-line logs. Otherwise, each line of a multi-line log will be treated as a separate log
  - Copy the `spark_linestart` file from [this directory](./data-analysis/src/main/resources/loghub) to the `Spark` subdirectory of your LogHub folder
  - Copy the `mac_linestart` file from [this directory](./data-analysis/src/main/resources/loghub) to the `Mac` subdirectory of your LogHub folder
  - Copy the `hadoop_linestart` file from [this directory](./data-analysis/src/main/resources/loghub) to the `Hadoop` and `HDFS_2` subdirectories of your LogHub folder
- Generate test data-sets
  - From the repository root dir execute command `./scripts/generate_data.sh`
- You should now have a `data` directory within your repository root dir containing the `1M_generated` and `5M_generated` data sets
  - NOTE: The number of log lines and the size of the resulting data set will vary slightly when generated multiple times

## Executing benchmarks

**Required**
- OpenJDK >= 11
    - Paper benchmarks were performed with OpenJDK 17.0.7
- Generated data sets in the `data` directory of the repository root directory

**Steps**
- From the repository root dir execute command `sudo ./scripts/execute_benchmarks.sh`
  - Root privileges are necessary because the benchmarks need to clear the systems page cache between query executions
  - NOTE: Executing the whole benchmark suite will take several hours
- You should now have a folder `results` in your repository root dir which contains the results of the individual benchmarks as JSON files

## Visualizing the results

**Required**
- Python 3.11 (tested version 3.11.3)
- pipenv (tested version 2023.5.19)
- Benchmark results in the `results` directory of the repository root directory

**Steps**
- From the `visualizations` directory in the repository root execute `pipenv sync` to create a virtual Python environment with all necessary packages
- From the repository root execute `./scripts/generate_charts.sh`
- The `charts` directory in the repository root should now contain the same charts as used in the paper
