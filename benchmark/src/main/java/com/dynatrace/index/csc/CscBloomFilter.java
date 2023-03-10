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
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;

/**
 * Implementation of the "Circular Shift And Coalesce Bloom Filter" from the paper
 * "Building Fast and Compact Sketches for Approximately Multi-Set Multi-Membership Querying" by Rundong Li et.al.
 */
public final class CscBloomFilter implements CscFilter {

  private final MemoryBitSet[] repetitions;
  private final Hasher32[][] locationHashes;
  private final Hasher32[] partitionHashes;
  private final int[][] locationHashSeeds;
  private final int repetitionCapacity;
  private final int partitions;
  private final int sets;
  @Nullable
  private final Runnable closer;
  private final int capacityMask;
  private final int partitionMask;
  private final int[][][] partitionFunction;
  private final byte[] encodingBuffer = new byte[4];

  private CscBloomFilter(
      int repetitionCapacity,
      MemoryBitSet[] repetitions,
      Hasher32[][] locationHashes,
      Hasher32[] partitionHashes,
      int[][] locationHashSeeds,
      int[][][] partitionFunction,
      int partitions,
      int sets,
      @Nullable Runnable closer) {

    this.repetitionCapacity = repetitionCapacity;
    this.repetitions = repetitions;
    this.locationHashes = locationHashes;
    this.locationHashSeeds = locationHashSeeds;
    this.partitionHashes = partitionHashes;
    this.partitionFunction = partitionFunction;
    this.partitions = partitions;
    this.sets = sets;
    this.closer = closer;

    this.capacityMask = bitMask(repetitionCapacity);
    this.partitionMask = bitMask(partitions);
  }

  /**
   * Create a new CSC-BF instance.
   *
   * @param capacity capacity in bits of each repetition
   * @param hashes the number of hash functions to use in each repetition
   * @param partitions the number of partitions per repetition
   */
  public static CscFilter create(int capacity, int hashes, int repetitions, int partitions, int sets) {
    checkArgument(isPowerOfTwo(capacity), "capacity must be a power of 2");
    checkArgument(isPowerOfTwo(repetitions), "repetitions must be a power of 2");
    checkArgument(isPowerOfTwo(partitions), "partitions must be a power of 2");
    checkArgument(hashes > 0, "Hashes must be larger than 0");

    final int repetitionCapacity = capacity / repetitions;
    final MemoryBitSet[] repetitionBitSets = new MemoryBitSet[repetitions];
    final Hasher32[][] locationHashes = new Hasher32[repetitions][hashes];
    final int[][] locationHashSeeds = new int[repetitions][hashes];
    final Hasher32[] partitionHashes = createPartitionHashes(repetitions);

    final ThreadLocalRandom random = ThreadLocalRandom.current();
    for (int r = 0; r < repetitions; r++) {
      repetitionBitSets[r] = new MemoryBitSet(repetitionCapacity);

      // Location hashes
      Hasher32[] hashFunctions = new Hasher32[hashes];
      int[] seeds = new int[hashes];
      locationHashes[r] = hashFunctions;
      locationHashSeeds[r] = seeds;
      for (int i = 0; i < hashes; i++) {
        seeds[i] = random.nextInt();
        hashFunctions[i] = Hashing.murmur3_32(seeds[i]);
      }
    }

    final int[][][] partitionFunction = createPartitionFunction(partitionHashes, repetitions, partitions, sets);

    return new CscBloomFilter(
        repetitionCapacity,
        repetitionBitSets,
        locationHashes,
        partitionHashes,
        locationHashSeeds,
        partitionFunction,
        partitions,
        sets,
        null);
  }

  @Override
  public void update(byte[] bytes, int[] offsets, int[] lengths, int count, int posting) {
    for (int i = 0; i < count; i++) {
      update(bytes, offsets[i], lengths[i], posting);
    }
  }

  @Override
  public void update(byte[] bytes, int offset, int length, int posting) {
    for (int r = 0; r < repetitions.length; r++) {
      final MemoryBitSet repetition = repetitions[r];
      final Hasher32[] hashes = locationHashes[r];

      final int partition = powerOfTwoModulo(
          partitionHashes[r].hashBytesToInt(toBytes(posting, encodingBuffer)),
          partitionMask);
      for (Hasher32 hasher : hashes) {
        final int hash = hasher.hashBytesToInt(bytes, offset, length);
        final int anker = powerOfTwoModulo(hash, capacityMask);
        final int position = powerOfTwoModulo(anker + partition, capacityMask);
        repetition.set(position);
      }
    }
  }

  @Override
  public void query(byte[] bytes, IntConsumer postingsConsumer) {
    final MemoryBitSet result = queryInternal(bytes);
    consumeMatches(result, 0, postingsConsumer);
  }

  @Override
  public void queryAll(byte[][] bytes, IntConsumer postingsConsumer) {
    MemoryBitSet result = queryInternal(bytes[0]);
    for (int i = 1; i < bytes.length && !result.isEmpty(); i++) {
      MemoryBitSet tokenResult = queryInternal(bytes[i]);
      result.and(tokenResult);
    }

    consumeMatches(result, 0, postingsConsumer);
  }

  @Override
  public long estimatedMemoryUsageBytes() {
    long size = 0;
    for (MemoryBitSet repetition : repetitions) {
      size += repetition.estimatedMemoryUsage();
    }
    return size;
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
    IntEncoder.writeFullInt(buffer, 0, repetitions.length);
    out.write(buffer);
    IntEncoder.writeFullInt(buffer, 0, repetitionCapacity);
    out.write(buffer);
    IntEncoder.writeFullInt(buffer, 0, locationHashes[0].length);
    out.write(buffer);
    IntEncoder.writeFullInt(buffer, 0, partitions);
    out.write(buffer);
    IntEncoder.writeFullInt(buffer, 0, sets);
    out.write(buffer);

    // Write hash seeds
    for (int[] seeds : locationHashSeeds) {
      for (int seed : seeds) {
        IntEncoder.writeFullInt(buffer, 0, seed);
        out.write(buffer);
      }
    }

    // Write repetitions
    // All repetition byte buffers have the same capacity
    for (MemoryBitSet repetition : repetitions) {
      repetition.writeTo(out);
    }
  }

  public static CscFilter readFrom(FileInputStream in) throws IOException {
    // Read header
    final Header header = readHeader(in);

    // Read repetitions
    // Map main data into memory
    final int repetitionSize = header.repetitionCapacity / 8;

    final MappedByteBuffer mappedBuffer = in.getChannel().map(
        FileChannel.MapMode.READ_ONLY,
        in.getChannel().position(),
        (long) repetitionSize * header.repetitionCount);
    mappedBuffer.order(ByteOrder.LITTLE_ENDIAN);

    final Memory memory = Memory.readOnlyMappedMemory(mappedBuffer);
    final MemoryReader memoryReader = new MemoryReader(memory);

    final MemoryBitSet[] repetitions = new MemoryBitSet[header.repetitionCount];
    for (int i = 0; i < header.repetitionCount; i++) {
      final Memory repetitionMemory = memoryReader.createView(repetitionSize);
      repetitions[i] = new MemoryBitSet(repetitionMemory);
    }

    final Hasher32[] partitionHashes = createPartitionHashes(repetitions.length);
    final int[][][] partitionFunction = createPartitionFunction(
        partitionHashes, repetitions.length, header.partitions, header.sets);

    return new CscBloomFilter(
        header.repetitionCapacity,
        repetitions,
        header.locationHashes,
        partitionHashes,
        header.hashSeeds,
        partitionFunction,
        header.partitions,
        header.sets,
        () -> MappedBufferUtil.unmapBuffer(mappedBuffer));
  }

  public static CscFilter readFrom(InputStream in) throws IOException {
    // Read header
    final Header header = readHeader(in);

    // Read repetitions
    final int repetitionSize = header.repetitionCapacity / 8;
    final MemoryBitSet[] repetitions = new MemoryBitSet[header.repetitionCount];
    for (int i = 0; i < header.repetitionCount; i++) {
      final byte[] bytes = in.readNBytes(repetitionSize);
      repetitions[i] = new MemoryBitSet(Memory.readOnlyHeapMemory(bytes));
    }

    final Hasher32[] partitionHashes = createPartitionHashes(repetitions.length);
    final int[][][] partitionFunction = createPartitionFunction(
        partitionHashes, repetitions.length, header.partitions, header.sets);

    return new CscBloomFilter(
        header.repetitionCapacity,
        repetitions,
        header.locationHashes,
        partitionHashes,
        header.hashSeeds,
        partitionFunction,
        header.partitions,
        header.sets,
        null);
  }

  private static Header readHeader(InputStream in) throws IOException {
    byte[] buffer = new byte[4];

    // Read header
    in.readNBytes(buffer, 0, 4);
    final int repetitionCount = IntEncoder.readFullInt(buffer, 0);
    in.readNBytes(buffer, 0, 4);
    final int repetitionCapacity = IntEncoder.readFullInt(buffer, 0);
    in.readNBytes(buffer, 0, 4);
    final int hashCount = IntEncoder.readFullInt(buffer, 0);
    in.readNBytes(buffer, 0, 4);
    final int partitions = IntEncoder.readFullInt(buffer, 0);
    in.readNBytes(buffer, 0, 4);
    final int sets = IntEncoder.readFullInt(buffer, 0);

    // Read hashes
    final int[][] hashSeeds = new int[repetitionCount][hashCount];
    final Hasher32[][] repetitionHashes = new Hasher32[repetitionCount][hashCount];
    for (int i = 0; i < repetitionCount; i++) {
      for (int j = 0; j < hashCount; j++) {
        in.readNBytes(buffer, 0, 4);
        hashSeeds[i][j] = IntEncoder.readFullInt(buffer, 0);
        repetitionHashes[i][j] = Hashing.murmur3_32(hashSeeds[i][j]);
      }
    }

    return new Header(repetitionCount, repetitionCapacity, hashCount, partitions, sets, hashSeeds, repetitionHashes);
  }

  private MemoryBitSet queryInternal(byte[] bytes) {
    MemoryBitSet result = new MemoryBitSet(sets);
    MemoryBitSet temp = new MemoryBitSet(sets);

    queryRepetition(0, bytes, result::set);
    for (int i = 1; i < repetitions.length && !result.isEmpty(); i++) {
      queryRepetition(i, bytes, temp::set);
      result.and(temp);
    }

    return result;
  }

  private void queryRepetition(int repetitionIndex, byte[] bytes, IntConsumer setConsumer) {
    final MemoryBitSet repetition = repetitions[repetitionIndex];
    final Hasher32[] hashes = locationHashes[repetitionIndex];

    MemoryBitSet result = null;
    for (Hasher32 hasher : hashes) {
      final int hash = hasher.hashBytesToInt(bytes, 0, bytes.length);
      final int anker = powerOfTwoModulo(hash, capacityMask);

      final int endPosition = anker + partitions;
      final MemoryBitSet hashResult;
      if (endPosition < repetitionCapacity) {
        hashResult = repetition.get(anker, endPosition);
      } else {
        // Handle the circular shift
        hashResult = repetition.get(anker, repetitionCapacity);
        final MemoryBitSet shifted = repetition.get(0, endPosition - repetitionCapacity);
        consumeMatches(shifted, repetitionCapacity - anker, hashResult::set);
      }

      if (result == null) {
        result = hashResult;
      } else {
        result.and(hashResult);
      }
    }

    requireNonNull(result);
    for (int p = result.nextSetBit(0); p >= 0; p = result.nextSetBit(p + 1)) {
      for (int set : partitionFunction[repetitionIndex][p]) {
        setConsumer.accept(set);
      }
    }
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

  private static Hasher32[] createPartitionHashes(int repetitions) {
    final Hasher32[] hashes = new Hasher32[repetitions];
    for (int r = 0; r < repetitions; r++) {
      hashes[r] = Hashing.murmur3_32(r);
    }
    return hashes;
  }

  private static int[][][] createPartitionFunction(
      Hasher32[] partitionHashes, int repetitions, int partitions, int sets) {

    final byte[] encodingBuffer = new byte[4];
    final int partitionMask = bitMask(partitions);
    final IntArrayList[][] tempFunction = new IntArrayList[repetitions][partitions];
    for (int s = 0; s < sets; s++) {
      for (int r = 0; r < repetitions; r++) {
        final int partition = powerOfTwoModulo(
            partitionHashes[r].hashBytesToInt(toBytes(s, encodingBuffer)),
            partitionMask);
        IntArrayList partitionSets = tempFunction[r][partition];
        if (partitionSets == null) {
          partitionSets = new IntArrayList(4);
          tempFunction[r][partition] = partitionSets;
        }
        partitionSets.add(s);
      }
    }

    final int[][][] partitionFunction = new int[repetitions][partitions][];
    for (int r = 0; r < repetitions; r++) {
      for (int p = 0; p < partitions; p++) {
        final IntArrayList tempSets = tempFunction[r][p];
        if (tempSets != null) {
          partitionFunction[r][p] = tempSets.toArray();
        } else {
          partitionFunction[r][p] = new int[0];
        }
      }
    }

    return partitionFunction;
  }

  private static byte[] toBytes(int value, byte[] encodingBuffer) {
    IntEncoder.writeFullInt(encodingBuffer, 0, value);
    return encodingBuffer;
  }

  private static final class Header {
    final int repetitionCount;
    final int repetitionCapacity;
    final int hashCount;
    final int partitions;
    final int sets;
    final int[][] hashSeeds;
    final Hasher32[][] locationHashes;

    Header(
        int repetitionCount,
        int repetitionCapacity,
        int hashCount,
        int partitions,
        int sets,
        int[][] hashSeeds,
        Hasher32[][] locationHashes) {

      this.repetitionCount = repetitionCount;
      this.repetitionCapacity = repetitionCapacity;
      this.hashCount = hashCount;
      this.partitions = partitions;
      this.sets = sets;
      this.hashSeeds = hashSeeds;
      this.locationHashes = locationHashes;
    }
  }
}
