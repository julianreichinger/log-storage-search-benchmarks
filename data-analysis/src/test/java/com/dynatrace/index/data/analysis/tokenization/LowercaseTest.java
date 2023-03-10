package com.dynatrace.index.data.analysis.tokenization;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class LowercaseTest {

  @Test
  void lowerCaseMapping() {
    for (byte i = 'A'; i <= 'Z'; i++) {
      assertThat(Lowercase.toLowerCase(i)).isEqualTo((byte) (i + 32));
    }

    assertThat(Lowercase.toLowerCase((byte) '0')).isEqualTo((byte) '0');
    assertThat(Lowercase.toLowerCase((byte) -200)).isEqualTo((byte) -200);
  }

  @Test
  void transformByteArray() {
    byte[] utf8Bytes = "aAbBxXzZ01".getBytes(StandardCharsets.UTF_8);
    byte[] lowerCaseBuffer = new byte[100];

    Lowercase.toLowerCase(utf8Bytes, 0, 10, lowerCaseBuffer);
    assertThat(Arrays.copyOf(lowerCaseBuffer, 10)).isEqualTo("aabbxxzz01".getBytes(StandardCharsets.UTF_8));

    Lowercase.toLowerCase(utf8Bytes, 3, 4, lowerCaseBuffer);
    assertThat(Arrays.copyOf(lowerCaseBuffer, 4)).isEqualTo("bxxz".getBytes(StandardCharsets.UTF_8));
  }
}