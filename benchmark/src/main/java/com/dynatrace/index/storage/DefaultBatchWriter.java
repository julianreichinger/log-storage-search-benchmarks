package com.dynatrace.index.storage;

import static com.google.common.base.Preconditions.checkState;

import com.dynatrace.index.loggrep.LogGrepException;
import com.dynatrace.index.util.IntEncoder;
import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import javax.annotation.Nullable;

/**
 * Compresses each batch individually using z-standard. When the writer is flushed, all batches are combined
 * into a single file to avoid the overhead of opening many files during queries.
 */
public final class DefaultBatchWriter implements BatchWriter {

  static final String TMP_DIR = "tmp";
  static final String DATA_FILE = "data";
  static final String HEADER_FILE = "header";

  private final Path storagePath;
  private final Path tmpPath;
  private final byte[] writeBuffer;

  private OutputStream[] batches;
  private int[] originalBatchSizes;
  private int maxBatch;

  @Nullable
  private BatchReader reader;

  public DefaultBatchWriter(Path storagePath) {
    this.storagePath = storagePath;
    this.tmpPath = storagePath.resolve(TMP_DIR);
    this.writeBuffer = new byte[4];
    this.batches = new OutputStream[128];
    this.originalBatchSizes = new int[128];
  }

  @Override
  public void addLogLine(byte[] bytes, int offset, int length, int batch) {
    try {
      maxBatch = Math.max(maxBatch, batch);

      int entryLength = length + Integer.BYTES;
      OutputStream batchOut = acquireBatchWriter(batch, entryLength);
      IntEncoder.writeFullInt(writeBuffer, 0, length);
      batchOut.write(writeBuffer);
      batchOut.write(bytes, offset, length);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void flush() {
    final int[] batchOffsets = new int[maxBatch + 2];
    Arrays.fill(batchOffsets, -1);
    try (FileOutputStream dataOut = new FileOutputStream(storagePath.resolve(DATA_FILE).toFile());
         FileOutputStream headerOut = new FileOutputStream(storagePath.resolve(HEADER_FILE).toFile())) {

      final int maxOriginalBatchSize = Arrays.stream(originalBatchSizes).max().orElseThrow();
      writeDataFile(batchOffsets, dataOut);
      writeHeaderFile(batchOffsets, maxOriginalBatchSize, headerOut);

      reader = DefaultBatchReader.create(storagePath);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void writeHeaderFile(int[] batchOffsets, int maxOriginalBatchSize, FileOutputStream headerOut)
      throws IOException {

    final byte[] buffer = new byte[2 * Integer.BYTES + Integer.BYTES * batchOffsets.length];

    IntEncoder.writeFullInt(buffer, 0, maxOriginalBatchSize);
    IntEncoder.writeFullInt(buffer, Integer.BYTES, batchOffsets.length);
    int offset = 2 * Integer.BYTES;
    for (int batchOffset : batchOffsets) {
      IntEncoder.writeFullInt(buffer, offset, batchOffset);
      offset += Integer.BYTES;
    }

    headerOut.write(buffer);
  }

  private void writeDataFile(int[] batchOffsets, FileOutputStream dataOut) throws IOException {
    int offset = 0;
    for (int i = 0; i <= maxBatch; i++) {
      final OutputStream batch = batches[i];

      batchOffsets[i] = offset;
      if (batch != null) {
        final Path batchFile = tmpPath.resolve(String.valueOf(i));
        // Flush
        batch.flush();
        batch.close();

        // Copy data to single data file
        final long copiedBytes = Files.copy(batchFile, dataOut);
        offset += copiedBytes;

        // Delete tmp data
        batches[i] = null;
        originalBatchSizes[i] = 0;
        Files.deleteIfExists(batchFile);
      }
    }
    batchOffsets[maxBatch + 1] = offset;
    Files.deleteIfExists(tmpPath);
  }

  @Override
  public void readBatch(int batch, LogConsumer consumer) {
    checkState(reader != null, "Data not yet flushed");
    reader.readBatch(batch, consumer);
  }

  @Override
  public int getMaxBatch() {
    return maxBatch;
  }

  @Override
  public void close() {
    if (reader != null) {
      reader.close();
      reader = null;
    }
  }

  private OutputStream acquireBatchWriter(int batch, int length) {
    if (batch >= batches.length) {
      final int newSize = Math.max(batch + 1, batches.length * 2);
      batches = Arrays.copyOf(batches, newSize);
      originalBatchSizes = Arrays.copyOf(originalBatchSizes, newSize);
    }

    try {
      originalBatchSizes[batch] += length;
      OutputStream batchOut = batches[batch];
      if (batchOut == null) {
        Files.createDirectories(tmpPath);
        final Path batchFile = tmpPath.resolve(String.valueOf(batch));
        batchOut = new ZstdOutputStreamNoFinalizer(
            new FileOutputStream(batchFile.toFile()),
            RecyclingBufferPool.INSTANCE);
        batches[batch] = batchOut;
      }
      return batchOut;
    } catch (IOException e) {
      throw new LogGrepException(e);
    }
  }
}
