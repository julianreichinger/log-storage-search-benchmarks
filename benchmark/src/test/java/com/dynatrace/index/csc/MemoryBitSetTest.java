package com.dynatrace.index.csc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class MemoryBitSetTest {

  @Test
  void nextSetBit() {
    final MemoryBitSet bitSet = bitSet(1, 10, 127, 333);

    assertThat(bitSet.nextSetBit(0)).isOne();
    assertThat(bitSet.nextSetBit(1)).isOne();
    assertThat(bitSet.nextSetBit(2)).isEqualTo(10);
    assertThat(bitSet.nextSetBit(11)).isEqualTo(127);
    assertThat(bitSet.nextSetBit(128)).isEqualTo(333);
    assertThat(bitSet.nextSetBit(334)).isEqualTo(-1);
  }

  @Test
  void get() {
    final MemoryBitSet bitSet = bitSet(1, 10, 12, 127, 333);

    assertThat(collectBits(bitSet.get(0, 128))).containsExactly(1, 10, 12, 127);
    assertThat(collectBits(bitSet.get(0, 6400))).containsExactly(1, 10, 12, 127, 333);
    assertThat(collectBits(bitSet.get(10, 12))).containsExactly(0);
    assertThat(collectBits(bitSet.get(10, 13))).containsExactly(0, 2);
  }

  @Test
  void getFromEnd() {
    final MemoryBitSet bitSet = new MemoryBitSet(1024);
    bitSet.set(1020);
    bitSet.set(1023);
    assertThat(collectBits(bitSet.get(1019, 1024))).containsExactly(1, 4);
  }

  @Test
  void and() {
    assertThat(collectBits(bitSet(10, 11, 352, 500).and(bitSet()))).isEmpty();
    assertThat(collectBits(bitSet().and(bitSet(10, 11, 352, 500)))).isEmpty();
    assertThat(collectBits(bitSet(10, 11, 352, 500).and(bitSet(9, 11, 500, 1000)))).containsExactly(11, 500);
    assertThat(collectBits(bitSet(10, 11, 352, 500).and(bitSet(10, 11, 352, 500)))).containsExactly(10, 11, 352, 500);
  }

  private MemoryBitSet bitSet(int... bits) {
    final MemoryBitSet bitSet = new MemoryBitSet(0);
    for (int bit : bits) {
      bitSet.set(bit);
    }
    return bitSet;
  }

  private List<Integer> collectBits(MemoryBitSet bitSet) {
    List<Integer> result = new ArrayList<>();
    for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
      result.add(i);
    }
    return result;
  }
}