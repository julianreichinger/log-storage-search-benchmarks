package com.dynatrace.index.csc;

import static com.dynatrace.index.storage.StorageDirectories.dataDirectory;
import static com.dynatrace.index.storage.StorageDirectories.indexDirectory;
import static java.util.Objects.requireNonNull;

import com.dynatrace.index.LogStoreReaderBase;
import com.dynatrace.index.data.analysis.tokenization.Tokenizer;
import com.dynatrace.index.storage.BatchReader;
import com.dynatrace.index.storage.DefaultBatchReader;
import com.dynatrace.index.tokenization.NGramTokenizer;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

/**
 * Wrapper class for a read-only instance of the {@link ShiftingBloomFilter} implementation.
 */
public final class CscLogStoreReader extends LogStoreReaderBase {

  private final CscFilter csc;
  private final Tokenizer ngramTokenizer;

  private CscLogStoreReader(CscFilter csc, BatchReader reader) {
    super(reader);
    this.csc = requireNonNull(csc);
    this.ngramTokenizer = NGramTokenizer.create();
  }

  @Override
  protected void locateTokenBatches(byte[] utf8Token, BitSet batches) {
    final List<byte[]> tokens = new ArrayList<>();
    tokens.add(utf8Token);
    ngramTokenizer.tokenize(utf8Token, (tokenType, offset, length) ->
        tokens.add(Arrays.copyOfRange(utf8Token, offset, offset + length)));
    csc.queryAll(tokens.toArray(new byte[0][]), batches::set);
  }

  @Override
  protected void locateContainsBatches(byte[] utf8String, BitSet batches) {
    final List<byte[]> tokens = new ArrayList<>();
    ngramTokenizer.tokenize(utf8String, (tokenType, offset, length) ->
        tokens.add(Arrays.copyOfRange(utf8String, offset, offset + length)));
    csc.queryAll(tokens.toArray(new byte[0][]), batches::set);
  }

  @Override
  public long estimatedMemoryUsageBytes() {
    return csc.estimatedMemoryUsageBytes();
  }

  @Override
  public void close() {
    super.close();
    csc.close();
  }

  public static CscLogStoreReader loadFromDisk(Path storageDirectory) throws IOException {
    final Path indexDir = indexDirectory(storageDirectory);
    final Path dataDir = dataDirectory(storageDirectory);
    final BatchReader reader = DefaultBatchReader.create(dataDir);
    final File cscFile = indexDir.resolve(CscLogStore.FILE_NAME).toFile();
    try (FileInputStream in = new FileInputStream(cscFile)) {

      final CscFilter filter = readFilter(in);
      return new CscLogStoreReader(filter, reader);
    }
  }

  private static CscFilter readFilter(FileInputStream in) throws IOException {
    final int filterType = in.read();
    if (filterType == 0) {
      return ShiftingBloomFilter.readFrom(in);
    }
    return CscBloomFilter.readFrom(in);
  }
}
