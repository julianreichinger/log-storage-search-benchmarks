package com.dynatrace.index.csc;

import com.dynatrace.index.memory.Memory;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntConsumer;

/**
 * Re-implements parts of the functionality of the {@link java.util.BitSet} class. However, it operates on a
 * {@link Memory} instance, which makes it possible to operate on a mapped file instead of heap memory.
 */
public final class MemoryBitSet {

  private final Memory memory;

  private int size;

  MemoryBitSet(int bitCapacity) {
    this.memory = Memory.dynamicHeapMemory(bitCapacity >> 3);
  }

  MemoryBitSet(Memory memory) {
    this.memory = memory;
    this.size = memory.size();
  }

  void set(long position) {
    int wordOffset = wordOffset(position);
    bitwiseOr(wordOffset, 1L << position);
  }

  /**
   * Updates this bit set instance with the result of the Boolean "and" combination of this instance
   * with the other instance.
   *
   * @param other the other bit set
   * @return this instance
   */
  MemoryBitSet and(MemoryBitSet other) {
    final int minSize = Math.min(size, other.size);

    for (int offset = minSize; offset < size; offset += Long.BYTES) {
      setWord(offset, 0L);
    }

    for (int offset = 0; offset < minSize; offset += Long.BYTES) {
      final long thisValue = memory.getLong(offset);
      final long otherValue = other.memory.getLong(offset);

      setWord(offset, thisValue & otherValue);
    }

    return this;
  }

  /**
   * Create a new bit set instance consisting of the bits within the specified range.
   *
   * @param from start position (inclusive)
   * @param to end position (exclusive)
   * @return the new bitset
   */
  MemoryBitSet get(int from, int to) {
    // This code section is a modified copy of the Java BitSet method, except it uses different access methods for
    // words
    int bitSize = size * 8;

    if (bitSize <= from || from == to) {
      return new MemoryBitSet(0);
    }

    if (to > bitSize) {
      to = bitSize;
    }

    final MemoryBitSet result = new MemoryBitSet(to - from);
    final int targetWords = ((to - from - 1) >> 6) + 1;

    int sourceOffset = wordOffset(from);
    boolean wordAligned = ((from & 0x3f) == 0);

    // Process all words but the last word
    for (int offset = 0, i = 0;
         i < targetWords - 1;
         i++, offset += Long.BYTES, sourceOffset += Long.BYTES) {
      long word = wordAligned
          ? word(sourceOffset)
          : (word(sourceOffset) >>> from) | (word(sourceOffset + Long.BYTES) << -from);
      result.setWord(offset, word);
    }

    // Process the last word
    long lastWordMask = -1L >>> -to;
    long word;
    if (((to - 1) & 0x3f) < (from & 0x3f)) {
      word = (word(sourceOffset) >>> from) | ((word(sourceOffset + Long.BYTES) & lastWordMask) << -from);
    } else {
      word = (word(sourceOffset) & lastWordMask) >>> from;
    }
    result.setWord((targetWords - 1) << 3, word);

    return result;
  }

  /**
   * Find the next bit set to 1 from the specified position.
   *
   * @param from start position for the search (inclusive)
   * @return position of the next set bit, or -1 if no more bit is set
   */
  int nextSetBit(int from) {
    int wordOffset = wordOffset(from);

    if (wordOffset >= size) {
      return -1;
    }

    long word = word(wordOffset) & (-1L << from);
    while (true) {
      if (word != 0) {
        return (wordOffset * Byte.SIZE) + Long.numberOfTrailingZeros(word);
      }

      wordOffset += Long.BYTES;
      if (wordOffset >= size) {
        return -1;
      }

      word = word(wordOffset);
    }
  }

  boolean isEmpty() {
    return size == 0;
  }

  int estimatedMemoryUsage() {
    return memory.reservedBytes();
  }

  void writeTo(OutputStream out) throws IOException {
    memory.writeTo(out, 0, memory.reservedBytes());
  }

  @Override
  public String toString() {
    List<Integer> entries = new ArrayList<>();
    forEach(entries::add);
    return entries.toString();
  }

  private static int wordOffset(long bitPosition) {
    final long offset = bitPosition >> 6 << 3;
    if (offset > Integer.MAX_VALUE) {
      throw new IllegalStateException("Exceeded space limit for Java arrays.");
    }
    return (int) offset;
  }

  private void bitwiseOr(int wordOffset, long value) {
    long currentValue = wordOffset >= size
        ? 0L
        : word(wordOffset);
    setWord(wordOffset, currentValue | value);
  }

  private long word(int wordOffset) {
    return memory.getLong(wordOffset);
  }

  private void setWord(int wordOffset, long value) {
    memory.setLong(wordOffset, value);
    if (value != 0) {
      // We currently don't shrink the size anymore, as there is no use-case yet
      size = Math.max(size, wordOffset + Long.BYTES);
    }
  }

  private void forEach(IntConsumer consumer) {
    for (int i = nextSetBit(0); i >= 0; i = nextSetBit(i + 1)) {
      consumer.accept(i);
    }
  }
}
