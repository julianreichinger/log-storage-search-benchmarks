package com.dynatrace.index;

import com.dynatrace.index.csc.CscLogStore;
import com.dynatrace.index.csc.CscLogStoreReader;
import com.dynatrace.index.data.analysis.tokenization.Tokenizer;
import com.dynatrace.index.loggrep.LogGrepStore;
import com.dynatrace.index.loggrep.LogGrepStoreReader;
import com.dynatrace.index.lucene.LuceneLogStore;
import com.dynatrace.index.lucene.LuceneLogStoreReader;
import com.dynatrace.index.scan.ScanLogStore;
import com.dynatrace.index.scan.ScanLogStoreReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Locale;

public final class LogStoreFactory {

  private LogStoreFactory() {
    // static helper
  }

  public static LogStore createStore(
      String storeType, Path rootDir, Tokenizer tokenizer, int maxBatchCount, int cscSizeMB) {
    storeType = storeType.toLowerCase(Locale.ROOT);
    switch (storeType) {
      case "csc":
        return CscLogStore.create(rootDir,
            tokenizer,
            8 * cscSizeMB * 1024 * 1024, // size as bits
            4,
            1,
            maxBatchCount,
            maxBatchCount);
      case "csc-bf":
        return CscLogStore.create(rootDir,
            tokenizer,
            8 * cscSizeMB * 1024 * 1024, // size as bits
            4,
            2,
            maxBatchCount / 8,
            maxBatchCount);
      case "lucene":
        return createLuceneIndex(rootDir, tokenizer, maxBatchCount);
      case "loggrep":
        return new LogGrepStore(Path.of("./binaries/loggrep"), rootDir, 128_000);
      case "scan":
        return new ScanLogStore(rootDir, maxBatchCount);
      default:
        throw new IllegalArgumentException("Unknown index type: " + storeType);
    }
  }

  public static LogStoreReader loadReader(
      String storeType, Path rootDir) {
    storeType = storeType.toLowerCase(Locale.ROOT);
    switch (storeType) {
      case "csc": // intentional fall-through
      case "csc-bf":
        return loadCscReader(rootDir);
      case "lucene":
        return loadLuceneReader(rootDir);
      case "loggrep":
        return LogGrepStoreReader.loadFromDisk(Path.of("./binaries/loggrep"), rootDir);
      case "scan":
        return ScanLogStoreReader.loadFromDisk(rootDir);
      default:
        throw new IllegalArgumentException("Unknown index type: " + storeType);
    }
  }

  private static LogStoreReader loadCscReader(Path rootDir) {
    try {
      return CscLogStoreReader.loadFromDisk(rootDir);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static LuceneLogStoreReader loadLuceneReader(Path rootDir) {
    try {
      return LuceneLogStoreReader.loadFromDisk(rootDir);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static LuceneLogStore createLuceneIndex(Path rootDir, Tokenizer tokenizer, int maxBatchCount) {
    try {
      return LuceneLogStore.create(rootDir, tokenizer, maxBatchCount);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
