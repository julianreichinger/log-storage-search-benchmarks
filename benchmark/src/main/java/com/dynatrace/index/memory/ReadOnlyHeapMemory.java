package com.dynatrace.index.memory;

import static java.util.Objects.requireNonNull;

import com.dynatrace.index.util.IntEncoder;
import java.io.OutputStream;

/**
 * Read-only memory operating on a byte-array.
 */
final class ReadOnlyHeapMemory implements Memory {

  private final byte[] bytes;
  private final int baseOffset;
  private final int length;

  ReadOnlyHeapMemory(byte[] bytes) {
    this(bytes, 0, bytes.length);
  }

  private ReadOnlyHeapMemory(byte[] bytes, int baseOffset, int length) {
    this.bytes = requireNonNull(bytes);
    this.baseOffset = baseOffset;
    this.length = length;
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
    return IntEncoder.readFullLong(bytes, baseOffset + offset);
  }

  @Override
  public long getLong(int offset, int byteCount) {
    return IntEncoder.readLong(bytes, baseOffset + offset, byteCount);
  }

  @Override
  public int getInt(int offset) {
    return IntEncoder.readFullInt(bytes, baseOffset + offset);
  }

  @Override
  public int size() {
    return length;
  }

  @Override
  public int reservedBytes() {
    return length;
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
    return new ReadOnlyHeapMemory(bytes, baseOffset + offset, length);
  }

  @Override
  public void writeTo(OutputStream out, int offset, int length) {
    throw unsupportedOperation();
  }

  private static RuntimeException unsupportedOperation() {
    return new UnsupportedOperationException("Cannot modify read-only memory.");
  }
}
