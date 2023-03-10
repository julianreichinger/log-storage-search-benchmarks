package com.dynatrace.index.memory;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class DynamicHeapMemoryTest {

  @Test
  void growCapacityAsNeeded() {
    final DynamicHeapMemory memory = new DynamicHeapMemory(10);

    assertThat(memory.size()).isZero();
    assertThat(memory.reservedBytes()).isEqualTo(10);

    memory.setLong(9, 1, 1);
    assertThat(memory.getLong(9, 1)).isEqualTo(1);
    assertThat(memory.size()).isEqualTo(10);
    assertThat(memory.reservedBytes()).isEqualTo(10);

    memory.setLong(10, 1, 1);
    assertThat(memory.getLong(10, 1)).isEqualTo(1);
    assertThat(memory.size()).isEqualTo(11);
    assertThat(memory.reservedBytes()).isEqualTo(20);

    memory.setLong(100, 1, 1);
    assertThat(memory.getLong(100, 1)).isEqualTo(1);
    assertThat(memory.size()).isEqualTo(101);
    assertThat(memory.reservedBytes()).isEqualTo(101);
  }
}
