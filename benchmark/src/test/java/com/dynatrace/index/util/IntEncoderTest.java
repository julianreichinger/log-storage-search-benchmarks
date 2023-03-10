package com.dynatrace.index.util;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class IntEncoderTest {

  @Test
  void writeReadFullShort() {
    byte[] buffer = new byte[2];
    int value = 0x4567;
    IntEncoder.writeFullShort(buffer, 0, value);
    assertThat(buffer).isEqualTo(new byte[]{(byte) 0x67, (byte) 0x45});
    assertThat(IntEncoder.readFullShort(buffer, 0)).isEqualTo(value);
  }

  @Test
  void writeReadFullInt() {
    byte[] buffer = new byte[4];
    int value = 0x01234567;
    IntEncoder.writeFullInt(buffer, 0, value);
    assertThat(buffer).isEqualTo(new byte[]{(byte) 0x67, (byte) 0x45, (byte) 0x23, (byte) 0x01});
    assertThat(IntEncoder.readFullInt(buffer, 0)).isEqualTo(value);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4})
  void writeReadInts(int byteCount) {
    int bits = byteCount * 8;
    int maxValue = bits == Integer.SIZE
        ? Integer.MAX_VALUE
        : (1 << (byteCount * 8)) - 1;

    int[] values = {0, 1, maxValue / 2, maxValue};

    byte[] buffer = new byte[byteCount + 2];
    for (int value : values) {
      IntEncoder.writeInt(buffer, 0, byteCount, value);
      assertThat(IntEncoder.readInt(buffer, 0, byteCount)).isEqualTo(value);

      IntEncoder.writeInt(buffer, 2, byteCount, value);
      assertThat(IntEncoder.readInt(buffer, 2, byteCount)).isEqualTo(value);
    }
  }

  @Test
  void writeReadFullLong() {
    byte[] buffer = new byte[8];
    long value = 0x0123456789abcdefL;
    IntEncoder.writeFullLong(buffer, 0, value);
    assertThat(buffer).isEqualTo(new byte[]{
        (byte) 0xef, (byte) 0xcd, (byte) 0xab, (byte) 0x89, (byte) 0x67, (byte) 0x45, (byte) 0x23, (byte) 0x01
    });
    assertThat(IntEncoder.readFullLong(buffer, 0)).isEqualTo(value);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8})
  void writeReadLongs(int byteCount) {
    int bits = byteCount * 8;
    long maxValue = bits == Long.SIZE
        ? Long.MAX_VALUE
        : (1L << (byteCount * 8)) - 1;

    long[] values = {0, 1, maxValue / 2, maxValue};

    byte[] buffer = new byte[byteCount + 8];
    for (long value : values) {
      IntEncoder.writeLong(buffer, 0, byteCount, value);
      assertThat(IntEncoder.readLong(buffer, 0, byteCount)).isEqualTo(value);

      IntEncoder.writeLong(buffer, 2, byteCount, value);
      assertThat(IntEncoder.readLong(buffer, 2, byteCount)).isEqualTo(value);
    }
  }

  @Test
  void writeReadFullLongWithByteBuffer() {
    byte[] buffer = new byte[8];
    ByteBuffer view = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN);
    long value = 0x0123456789abcdefL;
    IntEncoder.writeFullLong(buffer, 0, value);
    assertThat(buffer).isEqualTo(new byte[]{
        (byte) 0xef, (byte) 0xcd, (byte) 0xab, (byte) 0x89, (byte) 0x67, (byte) 0x45, (byte) 0x23, (byte) 0x01
    });
    assertThat(IntEncoder.readFullLong(view, 0)).isEqualTo(value);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8})
  void writeReadLongsWithByteBuffer(int byteCount) {
    int bits = byteCount * 8;
    long maxValue = bits == Long.SIZE
        ? Long.MAX_VALUE
        : (1L << (byteCount * 8)) - 1;

    long[] values = {0, 1, maxValue / 2, maxValue};

    byte[] buffer = new byte[byteCount + 8];
    ByteBuffer view = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN);
    for (long value : values) {
      IntEncoder.writeLong(buffer, 0, byteCount, value);
      assertThat(IntEncoder.readLong(view, 0, byteCount)).isEqualTo(value);

      IntEncoder.writeLong(buffer, 2, byteCount, value);
      assertThat(IntEncoder.readLong(view, 2, byteCount)).isEqualTo(value);
    }
  }
}