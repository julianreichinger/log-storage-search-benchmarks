package com.dynatrace.index.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Locale;

public final class SystemUtils {

  private static final String OS_NAME = System.getProperty("os.name");
  private static final boolean IS_LINUX;

  static {
    IS_LINUX = OS_NAME.toLowerCase(Locale.ROOT).contains("linux");
  }

  public static boolean isLinux() {
    return IS_LINUX;
  }

  /**
   * Drops the pages caches of the operating system. Currently only works on Linux and has no effect on Windows
   * systems.
   */
  public static void dropPageCache() {
    if (!isLinux()) {
      // Currently not supported on Windows
      return;
    }

    try {
      Process process = Runtime.getRuntime()
          .exec("sync")
          .onExit()
          .join();

      if (process.exitValue() != 0) {
        throw new IllegalStateException("Clearing page cache failed with exit value " + process.exitValue());
      }

      process = Runtime.getRuntime()
          .exec("echo 1 > /proc/sys/vm/drop_caches")
          .onExit()
          .join();

      if (process.exitValue() != 0) {
        throw new IllegalStateException("Clearing page cache failed with exit value " + process.exitValue());
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private SystemUtils() {
    // static helper
  }
}
