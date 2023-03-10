package com.dynatrace.index.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class BoyerMooreTest {

  @Test
  void shouldMatchCaseInsensitiveAscii() {
    byte[] data = "This is Upper Case".getBytes(StandardCharsets.UTF_8);

    final BoyerMoore matcher1 = BoyerMoore.createForPattern("upper".getBytes(StandardCharsets.UTF_8));
    assertThat(matcher1.matchLowerCase(data, 0, data.length)).isEqualTo(8);

    final BoyerMoore matcher2 = BoyerMoore.createForPattern("upper case".getBytes(StandardCharsets.UTF_8));
    assertThat(matcher2.matchLowerCase(data, 0, data.length)).isEqualTo(8);
  }

  @Test
  void shouldMatchWithinBounds() {
    byte[] data = "look at my horse, my horse is amazing".getBytes(StandardCharsets.UTF_8);

    final BoyerMoore matcher = BoyerMoore.createForPattern("at my".getBytes(StandardCharsets.UTF_8));
    assertThat(matcher.matchLowerCase(data, 0, data.length)).isEqualTo(5);
    assertThat(matcher.matchLowerCase(data, 3, data.length)).isEqualTo(5);
    assertThat(matcher.matchLowerCase(data, 10, data.length)).isEqualTo(-1);
    assertThat(matcher.matchLowerCase(data, 0, 5)).isEqualTo(-1);
  }
}