package com.dynatrace.index.storage;

import static com.dynatrace.index.data.analysis.tokenization.Lowercase.toLowerCase;

import java.util.Arrays;

/**
 * Implementation of the Boyer-Moore algorithm for string pattern matching.
 * <p>
 * This algorithm is a modified version of the implementation provided
 * <a href="https://algs4.cs.princeton.edu/53substring/BoyerMoore.java.html">here</a>.
 */
public final class BoyerMoore {

  private final int[] right;
  private final byte[] pattern;

  private BoyerMoore(byte[] pattern, int[] right) {
    this.pattern = pattern;
    this.right = right;
  }

  public static BoyerMoore createForPattern(byte[] pattern) {
    final int[] right = preprocessPattern(pattern);
    return new BoyerMoore(pattern, right);
  }

  /**
   * Returns the index of the first occurrence of the pattern in the data. ASCII characters within the data
   * will be treated as lower-case, enabling case-insensitive matching when also encoding a lower-case pattern.
   *
   * @return the offset within the data array where the first match starts, or -1 if no match was found
   */
  public int matchLowerCase(byte[] data, int offset, int length) {
    final int patternLength = pattern.length;

    final int endOffset = Math.min(offset + length - patternLength, data.length - patternLength);
    int skip;
    for (int i = offset; i <= endOffset; i += skip) {
      skip = 0;
      for (int j = patternLength - 1; j >= 0; j--) {
        final byte lowerCaseByte = toLowerCase(data[i + j]);
        if (pattern[j] != lowerCaseByte) {
          skip = Math.max(1, j - right[lowerCaseByte & 0xff]);
          break;
        }
      }
      if (skip == 0) {
        // found pattern
        return i;
      }
    }

    return -1;
  }

  private static int[] preprocessPattern(byte[] pattern) {
    final int[] right = new int[256];
    Arrays.fill(right, -1);

    for (int j = 0; j < pattern.length; j++) {
      right[pattern[j] & 0xff] = j;
    }

    return right;
  }
}
