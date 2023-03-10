package com.dynatrace.index.data.analysis;

import com.dynatrace.index.data.analysis.cli.CliOptions;
import java.io.IOException;
import java.util.Arrays;

public final class Launcher {

  public static void main(String[] args) throws IOException {
    final String command = args[0];

    final String[] commandArgs = Arrays.copyOfRange(args, 1, args.length);
    final CliOptions options = CliOptions.parse(commandArgs);
    switch (command) {
      case "analyzeGroups":
        GroupAnalyzer.execute(options);
        break;
      case "analyzeTokens":
        TokenAnalyzer.execute(options);
        break;
      case "generate":
        LogGenerator.execute(options);
        break;
      case "shuffle":
        LineShuffler.execute(options);
        break;
      default:
        throw new IllegalArgumentException("Unknown command: " + command);
    }
  }
}
