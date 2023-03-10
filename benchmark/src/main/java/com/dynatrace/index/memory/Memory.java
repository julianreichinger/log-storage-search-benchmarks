package com.dynatrace.index.memory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.MappedByteBuffer;

/**
 * Provides an abstraction layer between data-structures and physical memory and enables other code to easily work with
 * different types of memory (heap, mapped, direct,...).
 *
 * <p>Implementations of this interface do not have to take any measures to ensure thread-safety, although
 * read-only implementations are typically thread-safe in the sense that they are immutable.
 * <p>Implementations will typically not perform any range checks for performance reasons.
 */
public interface Memory {

  /**
   * The access mode defines which operations on the memory are allowed and which will fail.
   */
  AccessMode accessMode();

  /**
   * Little-endian encode the value into the memory at the given offset.
   *
   * @throws UnsupportedOperationException if {@link #accessMode()} is not {@link AccessMode#READ_WRITE}
   */
  void setLong(int offset, long value);

  /**
   * Little-endian encode the least-significant bytes of the value into the memory at the given offset.
   *
   * @throws UnsupportedOperationException if {@link #accessMode()} is not {@link AccessMode#READ_WRITE}
   */
  void setLong(int offset, int byteCount, long value);

  /**
   * Little-endian decode the value from the memory at the given offset.
   */
  long getLong(int offset);

  /**
   * Little-endian decode the least-significant bytes of the value from the memory at the given offset.
   */
  long getLong(int offset, int byteCount);

  /**
   * Little-endian decode the 4 byte int - value from the memory at the given offset.
   */
  int getInt(int offset);

  /**
   * The amount of "relevant" bytes in the memory. Writable memory might reserve additional bytes which aren't counted
   * in the size until they are actually modified.
   */
  int size();

  /**
   * The amount of bytes reserved from the operating system, e.g. heap memory.
   */
  int reservedBytes();

  /**
   * Set all bytes of the memory to zero.
   *
   * @throws UnsupportedOperationException if {@link #accessMode()} is not {@link AccessMode#READ_WRITE}
   */
  void clear();

  /**
   * Copy the data from some source memory into this memory.
   *
   * @param dstOffset start offset within this memory
   * @param src source memory
   * @param srcOffset start offset within the source memory
   * @param length the number of bytes to copy
   * @throws UnsupportedOperationException if {@link #accessMode()} is not {@link AccessMode#READ_WRITE}
   */
  void copy(int dstOffset, Memory src, int srcOffset, int length);

  /**
   * Create a view of the current memory. While a length can be specified, views might not perform actual
   * range checks for performance reasons. Views operate on the same underlying data as their "parent" memory.
   *
   * @param offset "base" offset for the view. All data accesses will happen relative to this base offset.
   * @param length size of the view
   * @throws UnsupportedOperationException if {@link #accessMode()} is not {@link AccessMode#READ_ONLY}
   */
  Memory view(int offset, int length);

  default void writeTo(OutputStream out) throws IOException {
    writeTo(out, 0, size());
  }

  /**
   * Write (a part) of the memory to an output stream.
   *
   * @throws UnsupportedOperationException if {@link #accessMode()} is not {@link AccessMode#READ_WRITE}
   * @throws IOException in case the write operation fails
   */
  void writeTo(OutputStream out, int offset, int length) throws IOException;

  /**
   * Allocate new heap memory with some initial capacity. The returned memory instance will
   * have {@link AccessMode#READ_WRITE} and it will automatically grow if positions outside the current
   * capacity are written.
   *
   * @param initialCapacity the amount of heap memory to allocate initially
   * @return the allocated memory
   */
  static Memory dynamicHeapMemory(int initialCapacity) {
    return new DynamicHeapMemory(initialCapacity);
  }

  /**
   * Wrap the byte-array into a read-only memory instance with {@link AccessMode#READ_ONLY}.
   */
  static Memory readOnlyHeapMemory(byte[] bytes) {
    return new ReadOnlyHeapMemory(bytes);
  }

  /**
   * Wrap the byte-buffer into a read-only memory instance with {@link AccessMode#READ_ONLY}.
   */
  static Memory readOnlyMappedMemory(MappedByteBuffer buffer) {
    return new ReadOnlyBufferMemory(buffer, 0, buffer.capacity(), true);
  }

  enum AccessMode {
    READ_ONLY,
    READ_WRITE
  }
}
