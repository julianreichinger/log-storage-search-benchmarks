package com.dynatrace.index.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public final class FileUtils {

  private FileUtils() {
    // static helper
  }

  /**
   * Recursively calculate the total size of all files within the given directory.
   */
  public static long directorySize(Path directory) throws IOException {
    try (Stream<Path> dirStream = Files.walk(directory)) {
      return dirStream.filter(Files::isRegularFile)
          .mapToLong(path -> path.toFile().length())
          .sum();
    }
  }
}
