package com.dynatrace.index.data.analysis.tokenization;

/**
 * Utility class for fast lowercase conversion.
 */
public class Lowercase {

  private static final byte[] LOWER = new byte[256];

  static {
    for (int c = 0; c < 255; c++) {
      LOWER[c] = (byte) c;
      if (c >= 'A' && c <= 'Z') {
        LOWER[c] = (byte) (c + 32);
      }
    }
  }

  private Lowercase() {
    // utility class
  }

  /**
   * Returns the lowercase representation for a single byte UTF-8 character.
   * @param b the utf-8 single byte representation of the character
   * @return the lower case representation
   */
  public static byte toLowerCase(byte b) {
    return LOWER[b & 0xff];
  }

  public static void toLowerCase(byte[] source, int sourceOffset, int length, byte[] destination) {
    int count = 0;
    for (int i = sourceOffset; i < sourceOffset + length; i++) {
      destination[count++] = toLowerCase(source[i]);
    }
  }
}
