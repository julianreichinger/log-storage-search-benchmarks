package com.dynatrace.index.data.analysis;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Objects.requireNonNull;

import com.dynatrace.dynahist.Histogram;
import com.dynatrace.index.data.analysis.cli.CliOptions;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Can be used to generate benchmark data sets from (open-source) log data.
 *
 * <p>Configuration parameters:
 * <ul>
 *   <li>"in" (required): directory with source logs
 *   <li>"out" (required): output file for the generated log data
 *   <li>"distributionIn" (default "1M_logs_distribution"): input file for the histogram
 *   describing the line distribution
 *   <li>"groups" (default 1000): number of groups to produce
 *   <li>"mixedGroups" (default false): if false, each group will only contain logs of a single application.
 *   <li>"shuffle" (default true): if true, the lines of the generated log data will be shuffled
 * </ul>
 *
 * <p>Required input directory organization:
 * <ul>
 *   <li>each direct child-directory of the input directory is treated as one application
 *   <li>each file ending with ".log" within an application directory is treated as a log file for this application
 *   <li>In order to correctly detect multi-line logs for applications, each application directory can contain a file
 *   ending with "linestart". This file must contain a valid Java regular expression which matches only lines which
 *   start a new log entry.
 *   <li>The open-source <a href="https://github.com/logpai/loghub">LogHub</a> data set can, for example,
 *   be used to generate test data sets with this class.
 *   <li>The resources folder of this project contains the necessary "linestart" files to correclty handle multi-line
 *   logs from the LogHub data set.
 * </ul>
 */
public final class LogGenerator {

  private static final Logger LOG = LogManager.getLogger(LogGenerator.class);

  private static final String INPUT_DIR_OPTION = "in";
  private static final String OUTPUT_FILE_OPTION = "out";
  private static final String DISTRIBUTION_INPUT_FILE_OPTION = "distributionIn";
  private static final String GROUP_COUNT_OPTION = "groups";
  private static final String MIXED_GROUPS = "mixedGroups";
  private static final String SHUFFLE_OPTION = "shuffle";

  private static final int GROUP_COUNT_DEFAULT = 1000;
  private static final boolean SHUFFLE_DEFAULT = true;
  private static final boolean MIXED_GROUPS_DEFAULT = false;

  private static final String SEPARATOR = ",";

  private LogGenerator() {
    // static helper
  }

  public static void execute(CliOptions options) throws IOException {
    final Path inputDir = options.getAsPath(INPUT_DIR_OPTION);
    final Path outputFile = options.getAsPath(OUTPUT_FILE_OPTION);
    final Path distributionPath = options.getAsPathOrDefault(DISTRIBUTION_INPUT_FILE_OPTION, null);
    final int groupCount = options.getAsIntOrDefault(GROUP_COUNT_OPTION, GROUP_COUNT_DEFAULT);
    final boolean mixedGroups = options.getAsBooleanOrDefault(MIXED_GROUPS, MIXED_GROUPS_DEFAULT);
    final boolean shuffle = options.getAsBooleanOrDefault(SHUFFLE_OPTION, SHUFFLE_DEFAULT);

    final List<Path> applicationDirectories = listApplications(inputDir);
    final Histogram lineDistribution = loadDistribution(distributionPath);

    final Path tempFile = shuffle ? outputFile.getParent().resolve(UUID.randomUUID().toString()) : outputFile;

    ThreadLocalRandom random = ThreadLocalRandom.current();
    try (BufferedWriter writer = Files.newBufferedWriter(tempFile, WRITE, CREATE, TRUNCATE_EXISTING)) {
      for (int group = 0; group < groupCount; group++) {
        final long lineCount = Math.max(1L, (long) lineDistribution.getQuantile(random.nextDouble()));

        if (mixedGroups) {
          // Generate logs which
          generateMixedLogs(writer, applicationDirectories, lineCount, group);
        } else {
          // Generate groups which consist only of data from a single application
          final Path applicationDir = applicationDirectories.get(random.nextInt(applicationDirectories.size()));
          generateApplicationLogs(writer, applicationDir, lineCount, group);
        }

        if ((group + 1) % 100 == 0) {
          LOG.info("Generated {} of {} groups.", group + 1, groupCount);
        }
      }
    }

    if (shuffle) {
      LOG.info("Shuffling generated logs...");

      LineShuffler.shuffleLines(tempFile, outputFile);
      Files.delete(tempFile);
    }
  }

  private static void generateMixedLogs(
      BufferedWriter writer,
      List<Path> applicationDirectories,
      long lineCount,
      int groupId) throws IOException {

    final ThreadLocalRandom random = ThreadLocalRandom.current();

    int writtenLines = 0;
    while (writtenLines < lineCount) {
      // Choose a random application and add some lines from it to the current group
      Path applicationDir = applicationDirectories.get(random.nextInt(applicationDirectories.size()));
      long applicationLines = Math.min(20, lineCount - writtenLines);
      generateApplicationLogs(writer, applicationDir, applicationLines, groupId);

      writtenLines += applicationLines;
    }
  }

  @SuppressWarnings("resource") // ignore returned file channel self-reference
  private static void generateApplicationLogs(BufferedWriter writer, Path applicationDir, long lineCount, int groupId)
      throws IOException {
    final List<Path> applicationLogs = listLogFiles(applicationDir);
    final Pattern logLinePattern = loadLogLinePattern(applicationDir);
    final ThreadLocalRandom random = ThreadLocalRandom.current();

    int writtenLines = 0;
    int fileIndex = random.nextInt(applicationLogs.size());
    while (writtenLines < lineCount) {
      final Path applicationLog = applicationLogs.get(fileIndex);
      final long fileSize = Files.size(applicationLog);
      try (FileInputStream inputStream = new FileInputStream(applicationLog.toFile())) {
        // Skip to random position within log file
        inputStream.getChannel().position(random.nextLong(fileSize / 2));

        final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        // Read to the next log line
        final String startLine = readMultiLineLog(reader, logLinePattern, null, line -> { });

        // Transfer a consecutive section of log lines
        writtenLines += transferLines(reader, writer, startLine, logLinePattern, lineCount - writtenLines, groupId);
      }

      // Go to next file, if the application log did not include enough lines
      fileIndex = (fileIndex + 1) % applicationLogs.size();
    }
  }

  private static int transferLines(
      BufferedReader reader,
      BufferedWriter writer,
      @Nullable String startLine,
      @Nullable Pattern logLinePattern,
      long lineCount,
      int groupId) throws IOException {

    final String groupString = String.valueOf(groupId);
    Consumer<String> logLineConsumer = line -> {
      try {
        writer.write(groupString);
        writer.write(SEPARATOR);
        writer.write(line);
        writer.newLine();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };

    int writtenLines = 0;
    String line = startLine;
    while (writtenLines < lineCount && line != null) {
      line = readMultiLineLog(reader, logLinePattern, line, logLineConsumer);
      writtenLines++;
    }

    return writtenLines;
  }

  private static String readMultiLineLog(
      BufferedReader reader, @Nullable Pattern logLinePattern, @Nullable String startLine, Consumer<String> logConsumer)
      throws IOException {

    String line = startLine == null ? reader.readLine() : startLine;
    if (logLinePattern == null) {
      logConsumer.accept(line);
      return reader.readLine();
    }

    StringBuilder multiLineBuilder = new StringBuilder(line);
    line = reader.readLine();
    while (line != null && !logLinePattern.matcher(line).find()) {
      multiLineBuilder
          .append(' ')
          .append(line);

      line = reader.readLine();
    }

    logConsumer.accept(multiLineBuilder.toString());
    return line;
  }

  @Nullable
  private static Pattern loadLogLinePattern(Path applicationDir) throws IOException {
    try (Stream<Path> dirStream = Files.walk(applicationDir)) {
      final Path linePatternFile = dirStream
          .filter(Files::isRegularFile)
          .filter(f -> f.toFile().getName().endsWith("linestart"))
          .findFirst()
          .orElse(null);

      if (linePatternFile == null) {
        return null;
      }

      final String linePatternString = Files.readString(linePatternFile, StandardCharsets.UTF_8);
      return Pattern.compile(linePatternString);
    }
  }

  private static List<Path> listLogFiles(Path applicationDir) throws IOException {
    try (Stream<Path> dirStream = Files.walk(applicationDir)) {
      return dirStream
          .filter(Files::isRegularFile)
          .filter(f -> f.toFile().getName().endsWith(".log"))
          .collect(Collectors.toList());
    }
  }

  private static List<Path> listApplications(Path parentDir) throws IOException {
    try (Stream<Path> dirStream = Files.list(parentDir)) {
      return dirStream.filter(Files::isDirectory).collect(Collectors.toList());
    }
  }

  private static Histogram loadDistribution(@Nullable Path distributionPath) throws IOException {
    if (distributionPath == null) {
      return loadDefaultDistribution();
    }

    try (InputStream input = Files.newInputStream(distributionPath, READ);
         DataInputStream dataInput = new DataInputStream(input)) {
      return Histogram.readAsPreprocessed(GroupAnalyzer.HISTOGRAM_LAYOUT, dataInput);
    }
  }

  private static Histogram loadDefaultDistribution() throws IOException {
    final ClassLoader classLoader = LogGenerator.class.getClassLoader();
    try (InputStream input = classLoader.getResourceAsStream("distribution/1M_logs_distribution");
         DataInputStream dataInput = new DataInputStream(requireNonNull(input))) {
      return Histogram.readAsPreprocessed(GroupAnalyzer.HISTOGRAM_LAYOUT, dataInput);
    }
  }
}
