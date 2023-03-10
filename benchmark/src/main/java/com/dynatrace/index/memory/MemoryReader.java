package com.dynatrace.index.memory;

import static java.util.Objects.requireNonNull;

/**
 * Allows to consume some {@link Memory} instance sequentially.
 */
public final class MemoryReader {

  private final Memory memory;
  private int position;

  /**
   * Create a new reader positioned at offset 0 of the memory.
   */
  public MemoryReader(Memory memory) {
    this.memory = requireNonNull(memory);
  }

  /**
   * Consume the next full long.
   */
  public long consumeLong() {
    long value = memory.getLong(position, Long.BYTES);
    position += Long.BYTES;
    return value;
  }

  /**
   * Consume the next bytes as a long.
   * @param byteCount number of bytes to consume
   */
  public long consumeLong(int byteCount) {
    long value = memory.getLong(position, byteCount);
    position += byteCount;
    return value;
  }

  /**
   * Consume the next 4 bytes as an int.
   */
  public int consumeInt() {
    int value = memory.getInt(position);
    position += Integer.BYTES;
    return value;
  }

  /**
   * @return the current position of the reader within the memory
   */
  public int position() {
    return position;
  }

  /**
   * Create a new memory view starting at the current position of the reader. After this operation, the position
   * of the reader will be advanced by the specified length.
   *
   * @param length The number of bytes consumed for the memory view.
   */
  public Memory createView(int length) {
    final Memory view = memory.view(position, length);
    position += length;
    return view;
  }


}
