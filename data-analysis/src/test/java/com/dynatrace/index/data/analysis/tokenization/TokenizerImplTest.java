package com.dynatrace.index.data.analysis.tokenization;

import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.ASCII_ALPHA_NUM;
import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.ASCII_OTHER;
import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.UNICODE;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import com.dynatrace.index.data.analysis.tokenization.Tokenizer.TokenConsumer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class TokenizerImplTest {

  @Test
  void baseTokens() {
    final Tokenizer tokenizer = Tokenizers.builder()
        .forwardAllBaseTypes()
        .build();
    final TokenConsumer consumer = mock(TokenConsumer.class);
    final byte[] bytes = "ascii--token 🙂🤔😉//:url".getBytes(StandardCharsets.UTF_8);

    tokenizer.tokenize(bytes, consumer);

    final InOrder inOrder = inOrder(consumer);
    inOrder.verify(consumer).accept(ASCII_ALPHA_NUM, 0, 5);
    inOrder.verify(consumer).accept(ASCII_OTHER, 5, 2);
    inOrder.verify(consumer).accept(ASCII_ALPHA_NUM, 7, 5);
    inOrder.verify(consumer).accept(ASCII_OTHER, 12, 1);
    inOrder.verify(consumer).accept(UNICODE, 13, 12);
    inOrder.verify(consumer).accept(ASCII_OTHER, 25, 3);
    inOrder.verify(consumer).accept(ASCII_ALPHA_NUM, 28, 3);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  void noTokens() {
    final Tokenizer tokenizer = Tokenizers.builder().build();
    final TokenConsumer consumer = mock(TokenConsumer.class);
    final byte[] bytes = "ascii--token 🙂🤔😉//:url".getBytes(StandardCharsets.UTF_8);

    tokenizer.tokenize(bytes, consumer);

    verifyNoInteractions(consumer);
  }

  @Test
  void singleBaseTokenType() {
    final Tokenizer tokenizer = Tokenizers.builder()
        .addForwardedBaseType(ASCII_ALPHA_NUM)
        .build();
    final TokenConsumer consumer = mock(TokenConsumer.class);
    final byte[] bytes = "ascii--token 🙂🤔😉//:url".getBytes(StandardCharsets.UTF_8);

    tokenizer.tokenize(bytes, consumer);

    final InOrder inOrder = inOrder(consumer);
    inOrder.verify(consumer).accept(ASCII_ALPHA_NUM, 0, 5);
    inOrder.verify(consumer).accept(ASCII_ALPHA_NUM, 7, 5);
    inOrder.verify(consumer).accept(ASCII_ALPHA_NUM, 28, 3);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  void addToTokenQueue() {
    final TokenQueue queue = mock(TokenQueue.class);
    final DerivedTokenizer derivedTokenizer = mock(DerivedTokenizer.class);
    final Tokenizer tokenizer = new TokenizerImpl(queue, List.of(), List.of(derivedTokenizer));
    final TokenConsumer consumer = mock(TokenConsumer.class);
    final byte[] bytes = "com.dynatrace.liisl".getBytes(StandardCharsets.UTF_8);

    tokenizer.tokenize(bytes, consumer);

    final InOrder inOrder = inOrder(queue);
    inOrder.verify(queue).push(ASCII_ALPHA_NUM, 0, 3);
    inOrder.verify(queue).processQueue(eq(bytes), argThat(list -> list.size() == 1), eq(consumer));
    inOrder.verify(queue).push(ASCII_OTHER, 3, 1);
    inOrder.verify(queue).processQueue(eq(bytes), argThat(list -> list.size() == 1), eq(consumer));
    inOrder.verify(queue).push(ASCII_ALPHA_NUM, 4, 9);
    inOrder.verify(queue).processQueue(eq(bytes), argThat(list -> list.size() == 1), eq(consumer));
    inOrder.verify(queue).push(ASCII_OTHER, 13, 1);
    inOrder.verify(queue).processQueue(eq(bytes), argThat(list -> list.size() == 1), eq(consumer));
    inOrder.verify(queue).push(ASCII_ALPHA_NUM, 14, 5);
    inOrder.verify(queue).processQueue(eq(bytes), argThat(list -> list.size() == 1), eq(consumer));

    inOrder.verifyNoMoreInteractions();
  }
}