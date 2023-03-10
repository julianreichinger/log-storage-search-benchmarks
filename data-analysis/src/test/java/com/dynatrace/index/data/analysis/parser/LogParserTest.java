package com.dynatrace.index.data.analysis.parser;

import static com.dynatrace.index.data.analysis.parser.LogLineReader.DEFAULT_BATCH_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import com.dynatrace.index.data.analysis.tokenization.Tokenizer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class LogParserTest {

  @Test
  void parseLogs() throws IOException {
    byte[] data = ("1,an arbitrary line\n"
        + "2,I think, therefore, I am.\n"
        + "2,some other line\n").getBytes(StandardCharsets.UTF_8);

    final Tokenizer tokenizer = mock(Tokenizer.class);
    final TokenSink tokenSink = mock(TokenSink.class);
    final List<byte[]> lines = new ArrayList<>();

    doAnswer(invocation -> {
      final byte[] line = invocation.getArgument(0);
      final int length = invocation.getArgument(2);
      lines.add(Arrays.copyOf(line, length));
      return null;
    }).when(tokenizer).tokenize(any(), anyInt(), anyInt(), any());

    try (ByteArrayInputStream inputStream = new ByteArrayInputStream(data)) {
      final LogParser logParser = LogParser.create(inputStream, DEFAULT_BATCH_SIZE, Integer.MAX_VALUE);
      assertThat(logParser.readBatch()).isTrue();

      logParser.parseBatch(tokenizer, tokenSink, true);

      assertThat(lines.get(0)).containsExactly("an arbitrary line".getBytes(StandardCharsets.UTF_8));
      assertThat(lines.get(1)).containsExactly("i think, therefore, i am.".getBytes(StandardCharsets.UTF_8));
      assertThat(lines.get(2)).containsExactly("some other line".getBytes(StandardCharsets.UTF_8));

      final InOrder inOrder = inOrder(tokenSink, tokenizer);
      inOrder.verify(tokenSink).startLine(any(), eq(1));
      inOrder.verify(tokenizer).tokenize(any(), eq(0), eq(17), any());
      inOrder.verify(tokenSink).endLine();

      inOrder.verify(tokenSink).startLine(any(), eq(2));
      inOrder.verify(tokenizer).tokenize(any(), eq(0), eq(25), any());
      inOrder.verify(tokenSink).endLine();

      inOrder.verify(tokenSink).startLine(any(), eq(2));
      inOrder.verify(tokenizer).tokenize(any(), eq(0), eq(15), any());
      inOrder.verify(tokenSink).endLine();
    }
  }
}