package com.dynatrace.index.tokenization;

import static java.util.Objects.requireNonNull;

import com.dynatrace.index.data.analysis.parser.TokenSink;
import com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Selects and collects tokens which are suitable for queries. Every query token is extended by special characters
 * to ensure no query token can never be a sub-string of another token. Otherwise, the calculation of the
 * false-positive-rate for queries would be incorrect.
 */
public final class QueryTokenSink implements TokenSink {

  private final int maxQueryTokens;
  private final Set<TokenKey> queryTokens;

  private byte[] utf8Bytes;

  public QueryTokenSink(int maxQueryTokens) {
    this.maxQueryTokens = maxQueryTokens;
    this.queryTokens = new HashSet<>(maxQueryTokens);
  }

  @Override
  public void startLine(byte[] utf8Bytes, int posting) {
    this.utf8Bytes = utf8Bytes;
  }

  @Override
  public void accept(TokenType tokenType, int offset, int length) {
    if (isPotentialQueryToken(length)) {
      collectQueryToken(offset, length);
    }
  }

  public Set<TokenKey> getQueryTokens() {
    return queryTokens;
  }

  private boolean isPotentialQueryToken(int length) {
    // Choose tokens which are likely to have a reasonable selectivity
    return length > 8;
  }

  private void collectQueryToken(int offset, int length) {
    // Remember the first N tokens and their groups for the queries
    if (queryTokens.size() < maxQueryTokens) {
      TokenKey tokenKey = new TokenKey(Arrays.copyOfRange(utf8Bytes, offset, offset + length));
      queryTokens.add(tokenKey);
    }
  }

  public static final class TokenKey {

    private final byte[] bytes;

    TokenKey(byte[] bytes) {
      this.bytes = requireNonNull(bytes);
    }

    public byte[] bytes() {
      return bytes;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TokenKey tokenKey = (TokenKey) o;

      return Arrays.equals(bytes, tokenKey.bytes);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
      return new String(bytes, StandardCharsets.UTF_8);
    }
  }
}
