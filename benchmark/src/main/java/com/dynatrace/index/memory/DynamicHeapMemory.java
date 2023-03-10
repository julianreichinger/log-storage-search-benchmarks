package com.dynatrace.index.memory;

import com.dynatrace.index.util.IntEncoder;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * Writable memory operating on a byte-array which will be grown dynamically when required.
 */
final class DynamicHeapMemory implements Memory {

  private static final int GROWTH_FACTOR = 2;

  private byte[] bytes;
  private int size;

  DynamicHeapMemory(int initialCapacity) {
    this.bytes = new byte[initialCapacity];
  }

  @Override
  public AccessMode accessMode() {
    return AccessMode.READ_WRITE;
  }

  @Override
  public void setLong(int offset, long value) {
    ensureCapacity(offset, Long.BYTES);
    IntEncoder.writeFullLong(bytes, offset, value);
  }

  @Override
  public void setLong(int offset, int byteCount, long value) {
    ensureCapacity(offset, byteCount);
    IntEncoder.writeLong(bytes, offset, byteCount, value);
  }

  @Override
  public long getLong(int offset) {
    return IntEncoder.readFullLong(bytes, offset);
  }

  @Override
  public long getLong(int offset, int byteCount) {
    return IntEncoder.readLong(bytes, offset, byteCount);
  }

  @Override
  public int getInt(int offset) {
    return IntEncoder.readFullInt(bytes, offset);
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public int reservedBytes() {
    return bytes.length;
  }

  @Override
  public void clear() {
    Arrays.fill(bytes, (byte) 0);
    size = 0;
  }

  @Override
  public void copy(int dstOffset, Memory src, int srcOffset, int length) {
    ensureCapacity(dstOffset, length);

    if (src instanceof DynamicHeapMemory) {
      DynamicHeapMemory heapSource = (DynamicHeapMemory) src;
      System.arraycopy(heapSource.bytes, srcOffset, bytes, dstOffset, length);
      return;
    }

    // Implement this once there is more than one modifiable memory type
    throw new UnsupportedOperationException("Generic copy logic not yet implemented.");
  }

  @Override
  public Memory view(int offset, int length) {
    throw new UnsupportedOperationException("Views are not supported for dynamic head memory");
  }

  @Override
  public void writeTo(OutputStream out, int offset, int length) throws IOException {
    out.write(bytes, offset, length);
  }

  private void ensureCapacity(int offset, int length) {
    if (offset + length > bytes.length) {
      int requiredCapacity = offset + length;
      int newCapacity = Math.max(requiredCapacity, bytes.length * GROWTH_FACTOR);
      bytes = Arrays.copyOf(bytes, newCapacity);
    }
    size = Math.max(size, offset + length);
  }
}
