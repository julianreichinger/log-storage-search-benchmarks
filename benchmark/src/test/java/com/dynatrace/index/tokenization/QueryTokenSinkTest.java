package com.dynatrace.index.tokenization;

import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.ASCII_ALPHA_NUM;
import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.COMBO;
import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.DOT_COMBO;
import static org.assertj.core.api.Assertions.assertThat;

import com.dynatrace.index.tokenization.QueryTokenSink.TokenKey;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class QueryTokenSinkTest {

  @Test
  void collectQueryTokens() {
    final QueryTokenSink tokenSink = new QueryTokenSink(100);

    byte[] line1 = "com.dynatrace.liisl".getBytes(StandardCharsets.UTF_8);
    tokenSink.startLine(line1, 0);
    // com - too short
    tokenSink.accept(ASCII_ALPHA_NUM, 0, 3);
    // dynatrace - usable query token
    tokenSink.accept(ASCII_ALPHA_NUM, 4, 9);
    // com.dynatrace.liisl - usable query token
    tokenSink.accept(DOT_COMBO, 0, 19);

    tokenSink.endLine();

    byte[] line2 = "aa.bb".getBytes(StandardCharsets.UTF_8);
    tokenSink.startLine(line2, 1);
    // aa.bb - too short
    tokenSink.accept(COMBO, 0, 5);

    tokenSink.endLine();

    assertThat(tokenSink.getQueryTokens()).contains(
        new TokenKey(toBytes("dynatrace")),
        new TokenKey(toBytes("com.dynatrace.liisl")));
  }

  private byte[] toBytes(String text) {
    return text.getBytes(StandardCharsets.UTF_8);
  }
}
