package com.dynatrace.index.data.analysis;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import com.dynatrace.dynahist.Histogram;
import com.dynatrace.dynahist.layout.Layout;
import com.dynatrace.dynahist.layout.OpenTelemetryExponentialBucketsLayout;
import com.dynatrace.index.data.analysis.cli.CliOptions;
import com.dynatrace.index.data.analysis.parser.LogParser;
import com.dynatrace.index.data.analysis.parser.TokenSink;
import com.dynatrace.index.data.analysis.tokenization.TokenQueue;
import com.dynatrace.index.data.analysis.tokenization.Tokenizer;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;

/**
 * Allows to analyze the line distribution between groups within a log data set. The produced histogram
 * can be stored and used as input for the {@link LogGenerator}. Can additionally store a configured amount of
 * quantiles from the distribution in a comma separated list for easy visualization.
 *
 * <p>Configuration parameters:
 * <ul>
 *   <li>"in" (required): input log file in format described below
 *   <li>"out" (optional): output file for the stored histogram
 *   <li>"quantilesOut" (optional): output file for the stored quantiles
 *   <li>"quantiles" (default 100): number of quantiles to store (0th and 100th percentile will always be stored)
 * </ul>
 *
 * <p>Required log file format:
 * <pre>
 * [integer group id],[textual log line]\n
 * [integer group id],[textual log line]\n
 * ...
 * </pre>
 */
final class GroupAnalyzer {

  private static final String INPUT_FILE_OPTION = "in";
  private static final String OUTPUT_FILE_OPTION = "out";
  private static final String PERCENTILES_OUTPUT_FILE_OPTION = "quantilesOut";
  private static final String PERCENTILES_OPTION = "quantiles";

  static final Layout HISTOGRAM_LAYOUT = OpenTelemetryExponentialBucketsLayout.create(10);

  private GroupAnalyzer() {
    // static helper
  }

  static void execute(CliOptions cliOptions) throws IOException {
    final Path inputFile = cliOptions.getAsPath(INPUT_FILE_OPTION);
    final Path outputFile = cliOptions.getAsPathOrDefault(OUTPUT_FILE_OPTION, null);

    final Histogram distribution = analyzeGroups(inputFile);

    if (outputFile != null) {
      try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
           DataOutputStream byteOutput = new DataOutputStream(byteStream)) {
        distribution.write(byteOutput);
        Files.write(outputFile, byteStream.toByteArray(), CREATE, WRITE, TRUNCATE_EXISTING);
      }
    }

    final Path percentilesOutputFile = cliOptions.getAsPathOrDefault(PERCENTILES_OUTPUT_FILE_OPTION, null);
    final int percentiles = cliOptions.getAsIntOrDefault(PERCENTILES_OPTION, 100);
    if (percentilesOutputFile != null) {
      writePercentiles(distribution, percentilesOutputFile, percentiles);
    }
  }

  static Histogram analyzeGroups(Path logFile) throws IOException {
    final IntLongHashMap groupLineCounts = new IntLongHashMap();
    LogParser.parseFile(logFile, GroupAnalyzer::tokenizeNothing, new TokenSink() {
      @Override
      public void startLine(byte[] utf8Bytes, int posting) {
        final long count = groupLineCounts.getIfAbsent(posting, 0);
        groupLineCounts.put(posting, count + 1);
      }

      @Override
      public void accept(TokenQueue.TokenType tokenType, int offset, int length) {
        // Nothing to do
      }
    });

    final Histogram histogram = Histogram.createStatic(HISTOGRAM_LAYOUT);

    groupLineCounts.forEachValue(histogram::addValue);

    return histogram.getPreprocessedCopy();
  }

  private static void writePercentiles(Histogram histogram, Path outputFile, int percentiles) throws IOException {
    final String csv = IntStream.range(0, percentiles + 1)
        .map(p -> (int) histogram.getQuantile(p / (double) percentiles))
        .mapToObj(String::valueOf)
        .collect(Collectors.joining(","));

    Files.write(outputFile, csv.getBytes(StandardCharsets.UTF_8), CREATE, WRITE, TRUNCATE_EXISTING);
  }

  private static void tokenizeNothing(byte[] bytes, int offset, int length, Tokenizer.TokenConsumer consumer) {
    // Do nothing - no tokens needed
  }
}
