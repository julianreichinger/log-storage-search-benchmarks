package com.dynatrace.index.memory;

import static java.util.Objects.requireNonNull;

import com.dynatrace.index.util.IntEncoder;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Read-only memory operating on some sort of {@link ByteBuffer} (heap, direct or mapped).
 */
final class ReadOnlyBufferMemory implements Memory {

  private final ByteBuffer buffer;
  private final int baseOffset;
  private final int length;
  private final boolean isMapped;

  ReadOnlyBufferMemory(ByteBuffer buffer, int baseOffset, int length, boolean isMapped) {
    this.buffer = requireNonNull(buffer);
    this.baseOffset = baseOffset;
    this.length = length;
    this.isMapped = isMapped;
  }

  @Override
  public AccessMode accessMode() {
    return AccessMode.READ_ONLY;
  }

  @Override
  public void setLong(int offset, long value) {
    throw unsupportedOperation();
  }

  @Override
  public void setLong(int offset, int byteCount, long value) {
    throw unsupportedOperation();
  }

  @Override
  public long getLong(int offset) {
    return IntEncoder.readFullLong(buffer, baseOffset + offset);
  }

  @Override
  public long getLong(int offset, int byteCount) {
    return IntEncoder.readLong(buffer, baseOffset + offset, byteCount);
  }

  @Override
  public int getInt(int offset) {
    return IntEncoder.readFullInt(buffer, baseOffset + offset);
  }

  @Override
  public int size() {
    return length;
  }

  @Override
  public int reservedBytes() {
    return isMapped ? 0 : length;
  }

  @Override
  public void clear() {
    throw unsupportedOperation();
  }

  @Override
  public void copy(int dstOffset, Memory src, int srcOffset, int length) {
    throw unsupportedOperation();
  }

  @Override
  public Memory view(int offset, int length) {
    return new ReadOnlyBufferMemory(buffer, baseOffset + offset, length, isMapped);
  }

  @Override
  public void writeTo(OutputStream out, int offset, int length) {
    throw unsupportedOperation();
  }

  private static RuntimeException unsupportedOperation() {
    return new UnsupportedOperationException("Cannot modify read-only memory.");
  }
}
