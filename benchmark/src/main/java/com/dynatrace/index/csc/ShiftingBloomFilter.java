package com.dynatrace.index.csc;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.math.IntMath.isPowerOfTwo;
import static java.util.Objects.requireNonNull;

import com.dynatrace.hash4j.hashing.Hasher32;
import com.dynatrace.hash4j.hashing.Hashing;
import com.dynatrace.index.memory.Memory;
import com.dynatrace.index.memory.MemoryReader;
import com.dynatrace.index.util.IntEncoder;
import com.dynatrace.index.util.MappedBufferUtil;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntConsumer;
import javax.annotation.Nullable;

/**
 * Implementation of a special case of the "Circular Shift And Coalesce Bloom Filter" from the paper
 * "Building Fast and Compact Sketches for Approximately Multi-Set Multi-Membership Querying" by Rundong Li et.al.
 * This implementation only uses a single repetition and is therefore equivalent to the "CSC-BIGSI"
 * and "Shifting Bloom Filter" variants mentioned in the paper.
 */
public final class ShiftingBloomFilter implements CscFilter {

  private final MemoryBitSet repetition;
  private final Hasher32[] hashes;
  private final int[] hashSeeds;
  private final int partitions;
  private final int capacity;
  private final int capacityMask;
  @Nullable
  private final Runnable closer;

  private ShiftingBloomFilter(
      int capacity,
      MemoryBitSet repetition,
      Hasher32[] hashes,
      int[] hashSeeds,
      int partitions,
      @Nullable Runnable closer) {

    this.capacity = capacity;
    this.repetition = repetition;
    this.hashes = hashes;
    this.hashSeeds = hashSeeds;
    this.partitions = partitions;
    this.closer = closer;

    this.capacityMask = bitMask(capacity);
  }

  /**
   * Create a new instance.
   *
   * @param capacity capacity in bits of each repetition
   * @param hashes the number of hash functions to use in each repetition
   * @param partitions the number of partitions per repetition
   */
  public static ShiftingBloomFilter create(int capacity, int hashes, int partitions) {
    checkArgument(isPowerOfTwo(capacity), "capacity must be a power of 2");
    checkArgument(isPowerOfTwo(partitions), "partitions must be a power of 2");
    checkArgument(hashes > 0, "Hashes must be larger than 0");

    final MemoryBitSet repetition = new MemoryBitSet(capacity);
    final Hasher32[] repetitionHashes = new Hasher32[hashes];
    final int[] hashSeeds = new int[hashes];

    final ThreadLocalRandom random = ThreadLocalRandom.current();
    for (int i = 0; i < hashes; i++) {
      hashSeeds[i] = random.nextInt();
      repetitionHashes[i] = Hashing.murmur3_32(hashSeeds[i]);
    }

    return new ShiftingBloomFilter(capacity, repetition, repetitionHashes, hashSeeds, partitions, null);
  }

  @Override
  public void update(byte[] bytes, int[] offsets, int[] lengths, int count, int posting) {
    for (int i = 0; i < count; i++) {
      update(bytes, offsets[i], lengths[i], posting);
    }
  }

  @Override
  public void update(byte[] bytes, int offset, int length, int posting) {
    for (Hasher32 hasher : hashes) {
      final int hash = hasher.hashBytesToInt(bytes, offset, length);
      final int anker = powerOfTwoModulo(hash, capacityMask);
      final int position = powerOfTwoModulo(anker + posting, capacityMask);
      repetition.set(position);
    }
  }

  @Override
  public void query(byte[] bytes, IntConsumer postingsConsumer) {
    final MemoryBitSet repetitionResults = queryInternal(bytes);
    consumeMatches(repetitionResults, 0, postingsConsumer);
  }

  @Override
  public void queryAll(byte[][] bytes, IntConsumer postingsConsumer) {
    MemoryBitSet result = queryInternal(bytes[0]);
    for (int i = 1; i < bytes.length; i++) {
      MemoryBitSet tokenResult = queryInternal(bytes[i]);

      result.and(tokenResult);
      if (result.isEmpty()) {
        // Stop early if query cannot match anymore
        return;
      }
    }

    consumeMatches(result, 0, postingsConsumer);
  }

  @Override
  public long estimatedMemoryUsageBytes() {
    return repetition.estimatedMemoryUsage();
  }

  @Override
  public void close() {
    if (closer != null) {
      closer.run();
    }
  }

  @Override
  public void writeTo(OutputStream out) throws IOException {
    byte[] buffer = new byte[4];

    // Write header
    IntEncoder.writeFullInt(buffer, 0, capacity);
    out.write(buffer);
    IntEncoder.writeFullInt(buffer, 0, hashes.length);
    out.write(buffer);
    IntEncoder.writeFullInt(buffer, 0, partitions);
    out.write(buffer);

    // Write hash seeds
    for (int seed : hashSeeds) {
      IntEncoder.writeFullInt(buffer, 0, seed);
      out.write(buffer);
    }

    // Write repetition
    repetition.writeTo(out);
  }

  public static ShiftingBloomFilter readFrom(FileInputStream in) throws IOException {
    // Read header
    final Header header = readHeader(in);

    // Read repetitions
    // Map main data into memory
    final int repetitionSize = header.capacity / 8;

    final MappedByteBuffer mappedBuffer = in.getChannel().map(
        FileChannel.MapMode.READ_ONLY,
        in.getChannel().position(),
        repetitionSize);
    mappedBuffer.order(ByteOrder.LITTLE_ENDIAN);

    final Memory memory = Memory.readOnlyMappedMemory(mappedBuffer);
    final MemoryReader memoryReader = new MemoryReader(memory);

    final Memory repetitionMemory = memoryReader.createView(repetitionSize);
    final MemoryBitSet repetition = new MemoryBitSet(repetitionMemory);

    return new ShiftingBloomFilter(
        header.capacity, repetition, header.repetitionHashes, header.hashSeeds, header.partitions,
        () -> MappedBufferUtil.unmapBuffer(mappedBuffer));
  }

  public static ShiftingBloomFilter readFrom(InputStream in) throws IOException {
    // Read header
    final Header header = readHeader(in);

    // Read repetitions
    final int repetitionSize = header.capacity / 8;
    final byte[] bytes = in.readNBytes(repetitionSize);
    final MemoryBitSet repetition = new MemoryBitSet(Memory.readOnlyHeapMemory(bytes));

    return new ShiftingBloomFilter(
        header.capacity, repetition, header.repetitionHashes, header.hashSeeds, header.partitions, null);
  }

  private static Header readHeader(InputStream in) throws IOException {
    byte[] buffer = new byte[4];

    // Read header
    in.readNBytes(buffer, 0, 4);
    final int capacity = IntEncoder.readFullInt(buffer, 0);
    in.readNBytes(buffer, 0, 4);
    final int hashCount = IntEncoder.readFullInt(buffer, 0);
    in.readNBytes(buffer, 0, 4);
    final int partitions = IntEncoder.readFullInt(buffer, 0);

    // Read hashes
    final int[] hashSeeds = new int[hashCount];
    final Hasher32[] repetitionHashes = new Hasher32[hashCount];
    for (int j = 0; j < hashCount; j++) {
      in.readNBytes(buffer, 0, 4);
      hashSeeds[j] = IntEncoder.readFullInt(buffer, 0);
      repetitionHashes[j] = Hashing.murmur3_32(hashSeeds[j]);
    }

    return new Header(capacity, hashCount, partitions, hashSeeds, repetitionHashes);
  }

  private MemoryBitSet queryInternal(byte[] bytes) {
    MemoryBitSet result = null;
    for (Hasher32 hasher : hashes) {
      final int hash = hasher.hashBytesToInt(bytes, 0, bytes.length);
      final int anker = powerOfTwoModulo(hash, capacityMask);

      final int endPosition = anker + partitions;
      final MemoryBitSet hashResult;
      if (endPosition < capacity) {
        hashResult = repetition.get(anker, endPosition);
      } else {
        // Handle the circular shift
        hashResult = repetition.get(anker, capacity);
        final MemoryBitSet shifted = repetition.get(0, endPosition - capacity);
        consumeMatches(shifted, capacity - anker, hashResult::set);
      }

      if (result == null) {
        result = hashResult;
      } else {
        result.and(hashResult);
      }
    }

    return requireNonNull(result);
  }

  private static int powerOfTwoModulo(int value, int bitMask) {
    // Faster alternative to modulo, since the capacity is guaranteed to be a power of 2
    return value & bitMask;
  }

  private static int bitMask(int partitions) {
    final int shift = Integer.numberOfLeadingZeros(partitions) + 1;
    return ~0 >>> shift;
  }

  private static void consumeMatches(MemoryBitSet result, int offset, IntConsumer postingsConsumer) {
    for (int i = result.nextSetBit(0); i >= 0; i = result.nextSetBit(i + 1)) {
      postingsConsumer.accept(offset + i);
    }
  }

  private static final class Header {
    final int capacity;
    final int hashCount;
    final int partitions;
    final int[] hashSeeds;
    final Hasher32[] repetitionHashes;

    Header(
        int capacity,
        int hashCount,
        int partitions,
        int[] hashSeeds,
        Hasher32[] repetitionHashes) {

      this.capacity = capacity;
      this.hashCount = hashCount;
      this.partitions = partitions;
      this.hashSeeds = hashSeeds;
      this.repetitionHashes = repetitionHashes;
    }
  }
}
