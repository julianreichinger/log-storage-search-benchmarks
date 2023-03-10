package com.dynatrace.index.csc;

import static com.dynatrace.index.storage.StorageDirectories.dataDirectory;
import static com.dynatrace.index.storage.StorageDirectories.indexDirectory;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import com.dynatrace.index.FinishTrace;
import com.dynatrace.index.LogStoreBase;
import com.dynatrace.index.data.analysis.tokenization.Tokenizer;
import com.dynatrace.index.storage.DefaultBatchWriter;
import com.dynatrace.index.tokenization.NGramTokenizer;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

/**
 * LogStore implementation using {@link ShiftingBloomFilter} instance to locate data.
 */
public class CscLogStore extends LogStoreBase {

  static final String FILE_NAME = "csc";

  private final CscFilter csc;
  private final Tokenizer ngramTokenizer;
  private final Path indexDirectory;

  CscLogStore(
      Path storageDirectory, DefaultBatchWriter writer, Tokenizer tokenizer, CscFilter csc, int maxBatchCount) {
    super(writer, dataDirectory(storageDirectory), tokenizer, csc::update, maxBatchCount);
    this.indexDirectory = indexDirectory(storageDirectory);
    this.ngramTokenizer = NGramTokenizer.create();
    this.csc = requireNonNull(csc);
  }

  public static CscLogStore create(
      Path storageDirectory, Tokenizer tokenizer, int capacity, int hashes, int repetitions, int partitions, int sets) {
    final CscFilter csc = createFilter(capacity, hashes, repetitions, partitions, sets);
    final DefaultBatchWriter writer = new DefaultBatchWriter(dataDirectory(storageDirectory));
    return new CscLogStore(storageDirectory, writer, tokenizer, csc, sets);
  }

  private static CscFilter createFilter(int capacity, int hashes, int repetitions, int partitions, int sets) {
    checkArgument(capacity > 0);
    checkArgument(repetitions > 0);
    checkArgument(hashes > 0);
    checkArgument(partitions > 0);

    if (repetitions == 1) {
      return ShiftingBloomFilter.create(capacity, hashes, partitions);
    }

    return CscBloomFilter.create(capacity, hashes, repetitions, partitions, sets);
  }

  @Override
  public void finish(FinishTrace trace) {
    try {
      Files.createDirectories(indexDirectory);

      trace.trackSketchMemoryUsage(csc.estimatedMemoryUsageBytes());
      final File cscFile = indexDirectory.resolve(FILE_NAME).toFile();
      final long sketchStart = System.nanoTime();
      try (FileOutputStream outputStream = new FileOutputStream(cscFile)) {
        writeFilterType(outputStream);
        csc.writeTo(outputStream);
        trace.trackSketchDiskUsage(outputStream.getChannel().position());
      }
      trace.trackSketchFinishTime(System.nanoTime() - sketchStart);

      super.finish(trace);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
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

  private void writeFilterType(FileOutputStream outputStream) throws IOException {
    if (csc instanceof ShiftingBloomFilter) {
      outputStream.write(0);
    } else {
      outputStream.write(1);
    }
  }
}
