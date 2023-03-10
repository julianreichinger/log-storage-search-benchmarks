package com.dynatrace.index.storage;

import static com.google.common.base.Preconditions.checkArgument;

import com.dynatrace.index.util.IntEncoder;
import com.github.luben.zstd.ZstdDecompressCtx;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import javax.annotation.Nullable;

/**
 * Used to read log batches produced by the {@link DefaultBatchWriter}.
 */
public final class DefaultBatchReader implements BatchReader {

  private final Path headerFile;
  private final Path dataFile;

  @Nullable
  private State state;

  public DefaultBatchReader(Path headerFile, Path dataFile) {
    this.headerFile = headerFile;
    this.dataFile = dataFile;
  }

  public static DefaultBatchReader create(Path storagePath) {
    final Path headerFile = storagePath.resolve(DefaultBatchWriter.HEADER_FILE);
    final Path dataFile = storagePath.resolve(DefaultBatchWriter.DATA_FILE);

    return new DefaultBatchReader(headerFile, dataFile);
  }

  @Override
  public void readBatch(int batch, LogConsumer consumer) {
    ensureState().readBatch(batch, consumer);
  }

  @Override
  public int getMaxBatch() {
    return ensureState().offsets.length - 2;
  }

  @Override
  public void close() {
    if (state == null) {
      return;
    }

    try {
      state.close();
      state = null;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private State ensureState() {
    if (state == null) {
      state = State.loadFrom(headerFile, dataFile);
    }
    return state;
  }

  private static final class State {
    final int[] offsets;
    final FileInputStream dataIn;

    final ZstdDecompressCtx decompressCtx;
    final byte[] decompressBuffer;

    State(int maxOriginalBatchSize, int[] offsets, FileInputStream dataIn) {
      this.offsets = offsets;
      this.dataIn = dataIn;

      this.decompressCtx = new ZstdDecompressCtx();
      this.decompressBuffer = new byte[maxOriginalBatchSize];
    }

    static State loadFrom(Path headerFile, Path dataFile) {
      try {
        final Header header = readHeader(headerFile);
        final FileInputStream dataIn = new FileInputStream(dataFile.toFile());

        return new State(header.maxOriginalBatchSize, header.offsets, dataIn);
      } catch (FileNotFoundException e) {
        throw new UncheckedIOException(e);
      }
    }

    void readBatch(int batch, LogConsumer consumer) {
      checkArgument(batch < offsets.length - 1 && batch >= 0,
          "Tried to access batch %s of %s", batch, offsets.length - 2);

      final int offset = offsets[batch];
      final int length = offsets[batch + 1] - offset;
      if (length == 0) {
        // Batch does not exist
        return;
      }

      try {
        // Load data
        dataIn.getChannel().position(offset);
        final byte[] compressedData = dataIn.readNBytes(length);

        // Decompress
        final int originalLength = decompressCtx.decompress(decompressBuffer, compressedData);
        int originalOffset = 0;
        while (originalOffset < originalLength) {
          int lineLength = IntEncoder.readFullInt(decompressBuffer, originalOffset);
          originalOffset += Integer.BYTES;

          consumer.acceptLog(decompressBuffer, originalOffset, lineLength);

          originalOffset += lineLength;
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    void close() throws IOException {
      dataIn.close();
      decompressCtx.close();
    }

    private static Header readHeader(Path offsetFile) {
      try (FileInputStream offsetsIn = new FileInputStream(offsetFile.toFile())) {
        final byte[] buffer = offsetsIn.readNBytes(8);
        final int maxOriginalBatchSize = IntEncoder.readFullInt(buffer, 0);
        final int length = IntEncoder.readFullInt(buffer, 4);

        final byte[] encodedOffsets = offsetsIn.readNBytes(length * Integer.BYTES);
        final int[] offsets = new int[length];
        int decodingOffset = 0;
        for (int i = 0; i < length; i++) {
          offsets[i] = IntEncoder.readFullInt(encodedOffsets, decodingOffset);
          decodingOffset += Integer.BYTES;
        }

        return new Header(maxOriginalBatchSize, offsets);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  private static final class Header {

    final int maxOriginalBatchSize;
    final int[] offsets;

    private Header(int maxOriginalBatchSize, int[] offsets) {
      this.maxOriginalBatchSize = maxOriginalBatchSize;
      this.offsets = offsets;
    }
  }
}
