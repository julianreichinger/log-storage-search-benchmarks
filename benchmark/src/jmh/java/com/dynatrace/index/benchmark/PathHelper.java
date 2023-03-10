package com.dynatrace.index.benchmark;

import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;

public class PathHelper {

  private static final String PROPERTY = "property:";
  public static final String TMP_DIR_PROPERTY = PROPERTY + "java.io.tmpdir";

  public static Path resolvePath(String rootPath) {
    Path path = isProperty(rootPath)
        ? Path.of(resolveProperty(rootPath))
        : Path.of(rootPath);

    String randomDir = String.valueOf(ThreadLocalRandom.current().nextInt());
    return path.resolve(randomDir);
  }

  private static boolean isProperty(String pathDescriptor) {
    return pathDescriptor.startsWith(PROPERTY);
  }

  private static String resolveProperty(String pathDescriptor) {
    return System.getProperty(pathDescriptor.substring(PROPERTY.length()));
  }
}
