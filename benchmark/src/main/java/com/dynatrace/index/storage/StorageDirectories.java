package com.dynatrace.index.storage;

import java.nio.file.Path;

public final class StorageDirectories {

  private StorageDirectories() {
    // static helper
  }

  public static Path dataDirectory(Path storageDirectory) {
    return storageDirectory.resolve("data");
  }

  public static Path indexDirectory(Path storageDirectory) {
    return storageDirectory.resolve("index");
  }
}
