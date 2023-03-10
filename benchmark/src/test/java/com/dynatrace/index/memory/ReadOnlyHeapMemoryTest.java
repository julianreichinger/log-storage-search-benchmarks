package com.dynatrace.index.memory;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ReadOnlyHeapMemoryTest {

  @Test
  void viewsAccessTheCorrectBytes() {
    final ReadOnlyHeapMemory memory = new ReadOnlyHeapMemory(new byte[]{0xC, 0xA, 0xF, 0xE, 0xB, 0xA, 0xB, 0xE});

    assertThat(memory.getLong(3, 1)).isEqualTo(0xE);
    assertThat(memory.size()).isEqualTo(8);
    assertThat(memory.reservedBytes()).isEqualTo(8);

    final Memory view1 = memory.view(2, memory.size() - 2);
    assertThat(view1.getLong(3, 1)).isEqualTo(0xA);
    assertThat(view1.size()).isEqualTo(6);
    assertThat(view1.reservedBytes()).isEqualTo(6);

    final Memory view2 = view1.view(2, view1.size() - 2);
    assertThat(view2.getLong(3, 1)).isEqualTo(0xE);
    assertThat(view2.size()).isEqualTo(4);
    assertThat(view2.reservedBytes()).isEqualTo(4);
  }
}