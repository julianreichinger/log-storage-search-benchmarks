package com.dynatrace.index.util;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Static helper methods for the encoding / decoding of integers.
 */
public final class IntEncoder {

  private static final VarHandle LONG_HANDLE =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle INT_HANDLE =
      MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle SHORT_HANDLE =
      MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.LITTLE_ENDIAN);

  private IntEncoder() {
    // static helper
  }

  /**
   * Write a full 2-byte short into a buffer at a given offset.
   */
  public static void writeFullShort(byte[] buffer, int offset, int value) {
    SHORT_HANDLE.set(buffer, offset, (short) value);
  }

  /**
   * Read a full 2-byte integer from a buffer at a given offset.
   */
  public static int readFullShort(byte[] buffer, int offset) {
    return (short) SHORT_HANDLE.get(buffer, offset) & 0xFFFF;
  }

  /**
   * Write a full 4-byte integer into a buffer at a given offset.
   */
  public static void writeFullInt(byte[] buffer, int offset, int value) {
    INT_HANDLE.set(buffer, offset, value);
  }

  /**
   * Read a full 4-byte integer from a buffer at a given offset.
   */
  public static int readFullInt(byte[] buffer, int offset) {
    return (int) INT_HANDLE.get(buffer, offset);
  }

  /**
   * Read a full 4-byte integer from a ByteBuffer at a given offset.
   */
  public static int readFullInt(ByteBuffer buffer, int offset) {
    return buffer.getInt(offset);
  }

  /**
   * Write the least significant bytes of an integer into a buffer at a given offset.
   */
  public static void writeInt(byte[] buffer, int offset, int byteCount, int value) {
    switch (byteCount) {
      case 1:
        buffer[offset] = (byte) value;
        break;
      case 2:
        SHORT_HANDLE.set(buffer, offset, (short) value);
        break;
      case 3:
        SHORT_HANDLE.set(buffer, offset, (short) value);
        buffer[offset + 2] = (byte) (value >>> 16);
        break;
      case 4:
        INT_HANDLE.set(buffer, offset, value);
        break;
      default:
        throw new IllegalArgumentException("max allowed byte count is 4, but was " + byteCount);
    }
  }

  /**
   * Read the least significant bytes of an integer from a buffer at a given offset.
   */
  public static int readInt(byte[] buffer, int offset, int byteCount) {
    switch (byteCount) {
      case 1:
        return (buffer[offset] & 0xff);
      case 2:
        return (short) SHORT_HANDLE.get(buffer, offset) & 0xFFFF;
      case 3:
        return ((short) SHORT_HANDLE.get(buffer, offset) & 0xFFFF)
            | ((buffer[offset + 2] & 0xff) << 16);
      case 4:
        return (int) INT_HANDLE.get(buffer, offset);
      default:
        throw new IllegalArgumentException("max allowed byte count is 4, but was " + byteCount);
    }
  }

  /**
   * Write a full 8-byte long into a buffer at a given offset.
   */
  public static void writeFullLong(byte[] buffer, int offset, long value) {
    LONG_HANDLE.set(buffer, offset, value);
  }

  /**
   * Read a full 8-byte long from a buffer at a given offset.
   */
  public static long readFullLong(byte[] buffer, int offset) {
    return (long) LONG_HANDLE.get(buffer, offset);
  }


  /**
   * Read a full 8-byte long from a buffer at a given offset.
   *
   * @param buffer the {@link ByteBuffer} has to be set to {@link ByteOrder#LITTLE_ENDIAN}, otherwise results
   *     will be incorrect
   */
  public static long readFullLong(ByteBuffer buffer, int offset) {
    return buffer.getLong(offset);
  }

  /**
   * Write the least significant bytes of a long into a buffer at a given offset.
   */
  public static void writeLong(byte[] buffer, int offset, int byteCount, long value) {
    switch (byteCount) {
      case 1:
        buffer[offset] = (byte) value;
        break;
      case 2:
        SHORT_HANDLE.set(buffer, offset, (short) value);
        break;
      case 3:
        SHORT_HANDLE.set(buffer, offset, (short) value);
        buffer[offset + 2] = (byte) (value >>> 16);
        break;
      case 4:
        INT_HANDLE.set(buffer, offset, (int) value);
        break;
      case 5:
        INT_HANDLE.set(buffer, offset, (int) value);
        buffer[offset + 4] = (byte) (value >>> 32);
        break;
      case 6:
        INT_HANDLE.set(buffer, offset, (int) value);
        SHORT_HANDLE.set(buffer, offset + 4, (short) (value >>> 32));
        break;
      case 7:
        INT_HANDLE.set(buffer, offset, (int) value);
        SHORT_HANDLE.set(buffer, offset + 4, (short) (value >>> 32));
        buffer[offset + 6] = (byte) (value >>> 48);
        break;
      case 8:
        LONG_HANDLE.set(buffer, offset, value);
        break;
      default:
        throw new IllegalArgumentException("max allowed byte count is 8, but was " + byteCount);
    }
  }

  /**
   * Read the least significant bytes of a long from a buffer at a given offset.
   */
  public static long readLong(byte[] buffer, int offset, int byteCount) {
    switch (byteCount) {
      case 1:
        return (buffer[offset] & 0xffL);
      case 2:
        return (short) SHORT_HANDLE.get(buffer, offset) & 0xffffL;
      case 3:
        return ((short) SHORT_HANDLE.get(buffer, offset) & 0xffffL)
            | ((buffer[offset + 2] & 0xffL) << 16);
      case 4:
        return (int) INT_HANDLE.get(buffer, offset) & 0xffffffffL;
      case 5:
        return (int) INT_HANDLE.get(buffer, offset) & 0xffffffffL
            | ((buffer[offset + 4] & 0xffL) << 32);
      case 6:
        return (int) INT_HANDLE.get(buffer, offset) & 0xffffffffL
            | (((short) SHORT_HANDLE.get(buffer, offset + 4) & 0xffffL) << 32);
      case 7:
        return (int) INT_HANDLE.get(buffer, offset) & 0xffffffffL
            | (((short) SHORT_HANDLE.get(buffer, offset + 4) & 0xffffL) << 32)
            | ((buffer[offset + 6] & 0xffL) << 48);
      case 8:
        return (long) LONG_HANDLE.get(buffer, offset);
      default:
        throw new IllegalArgumentException("max allowed byte count is 8, but was " + byteCount);
    }
  }

  /**
   * Read the least significant bytes of a long from a buffer at a given offset.
   *
   * @param buffer the {@link ByteBuffer} has to be set to {@link ByteOrder#LITTLE_ENDIAN}, otherwise results
   *     will be incorrect
   */
  public static long readLong(ByteBuffer buffer, int offset, int byteCount) {
    switch (byteCount) {
      case 1:
        return (buffer.get(offset) & 0xffL);
      case 2:
        return buffer.getShort(offset) & 0xffffL;
      case 3:
        return (buffer.getShort(offset) & 0xffffL)
            | ((buffer.get(offset + 2) & 0xffL) << 16);
      case 4:
        return buffer.getInt(offset) & 0xffffffffL;
      case 5:
        return buffer.getInt(offset) & 0xffffffffL
            | ((buffer.get(offset + 4) & 0xffL) << 32);
      case 6:
        return buffer.getInt(offset) & 0xffffffffL
            | ((buffer.getShort(offset + 4) & 0xffffL) << 32);
      case 7:
        return buffer.getInt(offset) & 0xffffffffL
            | ((buffer.getShort(offset + 4) & 0xffffL) << 32)
            | ((buffer.get(offset + 6) & 0xffL) << 48);
      case 8:
        return buffer.getLong(offset);
      default:
        throw new IllegalArgumentException("max allowed byte count is 8, but was " + byteCount);
    }
  }
}
