package com.dynatrace.index.data.analysis.tokenization;

import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.ASCII_ALPHA_NUM;
import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.ASCII_OTHER;
import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.UNICODE;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class BufferTokenQueueTest {

  @Test
  void matchSequence() {
    final BufferTokenQueue queue = new BufferTokenQueue();
    final int[] expected = {
        ASCII_ALPHA_NUM.getId(),
        ASCII_OTHER.getId(),
        ASCII_ALPHA_NUM.getId(),
        ASCII_OTHER.getId(),
        ASCII_ALPHA_NUM.getId()
    };

    assertThat(queue.matchesTypes(expected)).isFalse();

    queue.push(ASCII_ALPHA_NUM, 0, 2);
    queue.push(ASCII_OTHER, 2, 1);
    queue.push(ASCII_ALPHA_NUM, 3, 2);
    queue.push(ASCII_OTHER, 5, 1);
    assertThat(queue.matchesTypes(expected)).isFalse();

    queue.push(ASCII_ALPHA_NUM, 6, 2);
    assertThat(queue.matchesTypes(expected)).isTrue();

    queue.push(UNICODE, 8, 2);
    assertThat(queue.matchesTypes(expected)).isFalse();
  }

  @Test
  void insertMany() {
    final BufferTokenQueue queue = new BufferTokenQueue();
    for (int i = 0; i < 1000; i++) {
      queue.push(ASCII_ALPHA_NUM, i, 1);
    }

    final int[] expected = {ASCII_ALPHA_NUM.getId(), ASCII_ALPHA_NUM.getId()};
    assertThat(queue.matchesTypes(expected)).isTrue();
  }

  @Test
  void getOffsetAndLength() {
    final BufferTokenQueue queue = new BufferTokenQueue();

    queue.push(ASCII_ALPHA_NUM, 0, 2);
    queue.push(ASCII_OTHER, 2, 10);
    queue.push(ASCII_ALPHA_NUM, 12, 3);

    assertThat(queue.getOffset(0)).isEqualTo(12);
    assertThat(queue.getLength(0)).isEqualTo(3);

    assertThat(queue.getOffset(1)).isEqualTo(2);
    assertThat(queue.getLength(1)).isEqualTo(10);

    assertThat(queue.getOffset(2)).isZero();
    assertThat(queue.getLength(2)).isEqualTo(2);
  }
}