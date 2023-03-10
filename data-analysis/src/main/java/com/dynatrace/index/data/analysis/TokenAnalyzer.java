package com.dynatrace.index.data.analysis;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import com.dynatrace.dynahist.Histogram;
import com.dynatrace.dynahist.layout.Layout;
import com.dynatrace.dynahist.layout.OpenTelemetryExponentialBucketsLayout;
import com.dynatrace.hash4j.hashing.Hasher32;
import com.dynatrace.hash4j.hashing.Hashing;
import com.dynatrace.index.data.analysis.cli.CliOptions;
import com.dynatrace.index.data.analysis.parser.LogParser;
import com.dynatrace.index.data.analysis.parser.TokenSink;
import com.dynatrace.index.data.analysis.tokenization.TokenQueue;
import com.dynatrace.index.data.analysis.tokenization.Tokenizer;
import com.dynatrace.index.data.analysis.tokenization.TokenizerFactory;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;

/**
 * Allows to analyze the frequency distribution of tokens. Can additionally store a configured amount of
 * quantiles from the distribution in a comma separated list for easy visualization.
 *
 * <p>Configuration parameters:
 * <ul>
 *   <li>"in" (required): input log file in format described below
 *   <li>"out" (optional): output file for the stored histogram
 *   <li>"quantilesOut" (optional): output file for the stored quantiles
 *   <li>"quantiles" (default 100): number of quantiles to store (0th and 100th percentile will always be stored)
 *   <li>"tokenizer" (default "alphanumeric"): the tokenizer configuration to use for the analysis.
 *   Options are: "alphanumeric", "combo", "full"
 * </ul>
 *
 * <p>Required log file format:
 * <pre>
 * [integer group id],[textual log line]\n
 * [integer group id],[textual log line]\n
 * ...
 * </pre>
 */
public final class TokenAnalyzer {

  private static final String INPUT_FILE_OPTION = "in";
  private static final String OUTPUT_FILE_OPTION = "out";
  private static final String TOKENIZER_OPTION = "tokenizer";
  private static final String QUANTILES_OUTPUT_FILE_OPTION = "quantilesOut";
  private static final String QUANTILES_OPTION = "quantiles";

  private static final String TOKENIZER_DEFAULT = "alphanumeric";

  static final Layout HISTOGRAM_LAYOUT = OpenTelemetryExponentialBucketsLayout.create(10);

  private TokenAnalyzer() {
    // static helper
  }

  static void execute(CliOptions cliOptions) throws IOException {
    final Path inputFile = cliOptions.getAsPath(INPUT_FILE_OPTION);
    final Path outputFile = cliOptions.getAsPathOrDefault(OUTPUT_FILE_OPTION, null);
    final String tokenizerConfig = cliOptions.getAsStringOrDefault(TOKENIZER_OPTION, TOKENIZER_DEFAULT);

    final Tokenizer tokenizer = TokenizerFactory.createTokenizer(tokenizerConfig);
    final Histogram distribution = analyzeTokens(inputFile, tokenizer);

    if (outputFile != null) {
      try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
           DataOutputStream byteOutput = new DataOutputStream(byteStream)) {
        distribution.write(byteOutput);
        Files.write(outputFile, byteStream.toByteArray(), CREATE, WRITE, TRUNCATE_EXISTING);
      }
    }

    final Path percentilesOutputFile = cliOptions.getAsPathOrDefault(QUANTILES_OUTPUT_FILE_OPTION, null);
    final int percentiles = cliOptions.getAsIntOrDefault(QUANTILES_OPTION, 100);
    if (percentilesOutputFile != null) {
      writePercentiles(distribution, percentilesOutputFile, percentiles);
    }
  }

  static Histogram analyzeTokens(Path logFile, Tokenizer tokenizer) throws IOException {
    final Hasher32 hasher = Hashing.murmur3_32(0);
    final IntIntHashMap tokenCounts = new IntIntHashMap(100_000);
    LogParser.parseFile(logFile, tokenizer, new TokenSink() {

      private byte[] lineBytes;

      @Override
      public void startLine(byte[] utf8Bytes, int posting) {
        this.lineBytes = utf8Bytes;
      }

      @Override
      public void accept(TokenQueue.TokenType tokenType, int offset, int length) {
        int tokenHash = hasher.hashBytesToInt(lineBytes, offset, length);
        final int count = tokenCounts.getIfAbsent(tokenHash, 0);
        tokenCounts.put(tokenHash, count + 1);
      }
    });

    final Histogram histogram = Histogram.createStatic(HISTOGRAM_LAYOUT);

    tokenCounts.forEachValue(histogram::addValue);

    return histogram.getPreprocessedCopy();
  }

  private static void writePercentiles(Histogram histogram, Path outputFile, int percentiles) throws IOException {
    final String csv = IntStream.range(0, percentiles + 1)
        .map(p -> (int) histogram.getQuantile(p / (double) percentiles))
        .mapToObj(String::valueOf)
        .collect(Collectors.joining(","));

    Files.write(outputFile, csv.getBytes(StandardCharsets.UTF_8), CREATE, WRITE, TRUNCATE_EXISTING);
  }
}
