package com.dynatrace.index.csc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CscBloomFilterTest {

  private static final byte[] TOKEN_1 = "covfefe".getBytes(StandardCharsets.UTF_8);
  private static final byte[] TOKEN_2 = "com.dynatrace".getBytes(StandardCharsets.UTF_8);
  private static final byte[] TOKEN_3 = "hello".getBytes(StandardCharsets.UTF_8);
  private static final byte[] TOKEN_4 = "world".getBytes(StandardCharsets.UTF_8);

  @ParameterizedTest
  @MethodSource
  void shouldWriteReadPostings(FilterFactory factory) throws IOException {
    final CscFilter cscWriter = factory.create();

    cscWriter.update(TOKEN_1, 0, TOKEN_1.length, 10);
    cscWriter.update(TOKEN_2, 0, TOKEN_2.length, 20);
    cscWriter.update(TOKEN_3, 0, TOKEN_3.length, 30);
    cscWriter.update(TOKEN_4, 0, TOKEN_4.length, 40);
    cscWriter.update(TOKEN_1, 0, TOKEN_1.length, 15);
    cscWriter.update(TOKEN_2, 0, TOKEN_2.length, 15);

    assertWriteReadPostings(cscWriter);

    byte[] serializedCsc;
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      cscWriter.writeTo(out);
      serializedCsc = out.toByteArray();
    }

    CscFilter reader;
    try (ByteArrayInputStream in = new ByteArrayInputStream(serializedCsc)) {
      reader = factory.readFrom(in);
    }

    assertWriteReadPostings(reader);
  }

  private static Stream<Arguments> shouldWriteReadPostings() {
    final int capacity = 1024 * 1024;
    final int partitions = 128;
    return Stream.of(
        // ShiftingBloomFilter
        Arguments.of(new FilterFactory() {
          @Override
          public CscFilter create() {
            return ShiftingBloomFilter.create(capacity, 3, partitions);
          }

          @Override
          public CscFilter readFrom(InputStream in) throws IOException {
            return ShiftingBloomFilter.readFrom(in);
          }

          @Override
          public String toString() {
            return "ShiftingBloomFilter";
          }
        }),

        // CscBloomFilter
        Arguments.of(new FilterFactory() {
          @Override
          public CscFilter create() {
            return CscBloomFilter.create(capacity, 3, 2, partitions, partitions);
          }

          @Override
          public CscFilter readFrom(InputStream in) throws IOException {
            return CscBloomFilter.readFrom(in);
          }

          @Override
          public String toString() {
            return "CscBloomFilter";
          }
        })
    );
  }

  private void assertWriteReadPostings(CscFilter reader) {
    assertPostings(reader, TOKEN_1, 10, 15);
    assertPostings(reader, TOKEN_2, 15, 20);
    assertPostings(reader, TOKEN_3, 30);
    assertPostings(reader, TOKEN_4, 40);

    assertPostings(reader, new byte[][]{TOKEN_1, TOKEN_2}, 15);
  }

  private void assertPostings(CscFilter reader, byte[] queryToken, int... expectedPostings) {
    final BitSet result = new BitSet();
    final BitSet expected = toBitSet(expectedPostings);
    reader.query(queryToken, result::set);

    // Check that no expected posting was missing
    result.and(expected);
    assertThat(result).isEqualTo(expected);
  }

  private void assertPostings(CscFilter reader, byte[][] queryTokens, int... expectedPostings) {
    final BitSet result = new BitSet();
    final BitSet expected = toBitSet(expectedPostings);
    reader.queryAll(queryTokens, result::set);

    // Check that no expected posting was missing
    result.and(expected);
    assertThat(result).isEqualTo(expected);
  }

  private BitSet toBitSet(int[] expectedPostings) {
    BitSet result = new BitSet();
    for (int posting : expectedPostings) {
      result.set(posting);
    }
    return result;
  }

  private interface FilterFactory {

    CscFilter create();

    CscFilter readFrom(InputStream in) throws IOException;
  }
}