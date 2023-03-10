package com.dynatrace.index.data.analysis.tokenization;

import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.ASCII_ALPHA_NUM;
import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.ASCII_ALPHA_NUM_TRI_GRAM;
import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.ASCII_OTHER;
import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.ASCII_OTHER_NGRAM;
import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.COMBO;
import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.UNICODE;
import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.UNICODE_TWO_GRAM;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.dynatrace.index.data.analysis.tokenization.Tokenizer.TokenConsumer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class TokenizersTest {

  @Test
  void comboTerms() {
    final DerivedTokenizer tokenizer = Tokenizers::comboTerms;
    final TokenQueue queue = TokenQueue.createDefault();
    final TokenConsumer consumer = mock(TokenConsumer.class);

    // correct combo term
    final byte[] string1 = "aa.bb".getBytes(StandardCharsets.UTF_8);
    queue.push(ASCII_ALPHA_NUM, 0, 2);
    queue.push(ASCII_OTHER, 2, 1);
    queue.push(ASCII_ALPHA_NUM, 3, 2);
    tokenizer.processQueue(string1, queue, consumer);

    verify(consumer).accept(COMBO, 0, 5);
    verifyNoMoreInteractions(consumer);

    // wrong separator character
    queue.clear();
    final byte[] string2 = "aa=bb".getBytes(StandardCharsets.UTF_8);
    queue.push(ASCII_ALPHA_NUM, 0, 2);
    queue.push(ASCII_OTHER, 2, 1);
    queue.push(ASCII_ALPHA_NUM, 3, 2);
    tokenizer.processQueue(string2, queue, consumer);

    verifyNoMoreInteractions(consumer);

    // too long separator
    queue.clear();
    final byte[] string3 = "aa..bb".getBytes(StandardCharsets.UTF_8);
    queue.push(ASCII_ALPHA_NUM, 0, 2);
    queue.push(ASCII_OTHER, 2, 2);
    queue.push(ASCII_ALPHA_NUM, 4, 2);
    tokenizer.processQueue(string3, queue, consumer);

    verifyNoMoreInteractions(consumer);

    // wrong signature
    queue.clear();
    final byte[] string4 = "aa.".getBytes(StandardCharsets.UTF_8);
    queue.push(ASCII_ALPHA_NUM, 0, 2);
    queue.push(ASCII_OTHER, 2, 1);
    tokenizer.processQueue(string4, queue, consumer);

    verifyNoMoreInteractions(consumer);
  }

  @Test
  void dotComboTerms() {
    final DerivedTokenizer tokenizer = Tokenizers::dotComboTerms;
    final TokenQueue queue = TokenQueue.createDefault();
    final TokenConsumer consumer = mock(TokenConsumer.class);

    // correct dot combo term
    final byte[] string1 = "aa.bb.cc".getBytes(StandardCharsets.UTF_8);
    queue.push(ASCII_ALPHA_NUM, 0, 2);
    queue.push(ASCII_OTHER, 2, 1);
    queue.push(ASCII_ALPHA_NUM, 3, 2);
    queue.push(ASCII_OTHER, 5, 1);
    queue.push(ASCII_ALPHA_NUM, 6, 2);
    tokenizer.processQueue(string1, queue, consumer);

    verify(consumer).accept(TokenQueue.TokenType.DOT_COMBO, 0, 8);
    verifyNoMoreInteractions(consumer);

    // wrong separator character
    queue.clear();
    final byte[] string2 = "aa=bb=cc".getBytes(StandardCharsets.UTF_8);
    queue.push(ASCII_ALPHA_NUM, 0, 2);
    queue.push(ASCII_OTHER, 2, 1);
    queue.push(ASCII_ALPHA_NUM, 3, 2);
    queue.push(ASCII_OTHER, 5, 1);
    queue.push(ASCII_ALPHA_NUM, 6, 2);
    tokenizer.processQueue(string2, queue, consumer);

    verifyNoMoreInteractions(consumer);

    // too long separator
    queue.clear();
    final byte[] string3 = "aa..bb..cc".getBytes(StandardCharsets.UTF_8);
    queue.push(ASCII_ALPHA_NUM, 0, 2);
    queue.push(ASCII_OTHER, 2, 2);
    queue.push(ASCII_ALPHA_NUM, 4, 2);
    queue.push(ASCII_OTHER, 6, 2);
    queue.push(ASCII_ALPHA_NUM, 8, 2);
    tokenizer.processQueue(string3, queue, consumer);

    verifyNoMoreInteractions(consumer);

    // wrong signature
    queue.clear();
    final byte[] string4 = "aa.bb.".getBytes(StandardCharsets.UTF_8);
    queue.push(ASCII_ALPHA_NUM, 0, 2);
    queue.push(ASCII_OTHER, 2, 1);
    queue.push(ASCII_ALPHA_NUM, 3, 2);
    queue.push(ASCII_OTHER, 5, 1);
    tokenizer.processQueue(string4, queue, consumer);

    verifyNoMoreInteractions(consumer);
  }

  @Test
  void asciiTriGrams() {
    final DerivedTokenizer tokenizer = Tokenizers::asciiAlphaNumericTriGrams;
    final TokenQueue queue = TokenQueue.createDefault();
    final TokenConsumer consumer = mock(TokenConsumer.class);

    // correct term
    final byte[] string1 = "abcd".getBytes(StandardCharsets.UTF_8);
    queue.push(ASCII_ALPHA_NUM, 0, 4);
    tokenizer.processQueue(string1, queue, consumer);

    verify(consumer).accept(ASCII_ALPHA_NUM_TRI_GRAM, 0, 3);
    verify(consumer).accept(ASCII_ALPHA_NUM_TRI_GRAM, 1, 3);
    verifyNoMoreInteractions(consumer);

    // too short
    queue.clear();
    final byte[] string2 = "abc".getBytes(StandardCharsets.UTF_8);
    queue.push(ASCII_ALPHA_NUM, 0, 3);
    tokenizer.processQueue(string2, queue, consumer);

    verifyNoMoreInteractions(consumer);

    // wrong signature
    queue.clear();
    final byte[] string3 = "//::".getBytes(StandardCharsets.UTF_8);
    queue.push(ASCII_OTHER, 0, 4);
    tokenizer.processQueue(string3, queue, consumer);

    verifyNoMoreInteractions(consumer);
  }

  @Test
  void unicodeTwoGrams() {
    final DerivedTokenizer tokenizer = Tokenizers::unicodeTwoGrams;
    final TokenQueue queue = TokenQueue.createDefault();
    final TokenConsumer consumer = mock(TokenConsumer.class);

    // correct term (bytes are not actually checked, only the type)
    final byte[] string1 = "abc".getBytes(StandardCharsets.UTF_8);
    queue.push(UNICODE, 0, 3);
    tokenizer.processQueue(string1, queue, consumer);

    verify(consumer).accept(UNICODE_TWO_GRAM, 0, 2);
    verify(consumer).accept(UNICODE_TWO_GRAM, 1, 2);
    verifyNoMoreInteractions(consumer);

    // too short
    queue.clear();
    final byte[] string2 = "ab".getBytes(StandardCharsets.UTF_8);
    queue.push(UNICODE, 0, 2);
    tokenizer.processQueue(string2, queue, consumer);

    verifyNoMoreInteractions(consumer);

    // wrong signature
    queue.clear();
    final byte[] string3 = "//:".getBytes(StandardCharsets.UTF_8);
    queue.push(ASCII_OTHER, 0, 3);
    tokenizer.processQueue(string3, queue, consumer);

    verifyNoMoreInteractions(consumer);
  }

  @Test
  void asciiOtherNGrams() {
    final DerivedTokenizer tokenizer = Tokenizers::asciiOtherNGram;
    final TokenQueue queue = TokenQueue.createDefault();

    // correct term
    final byte[] string1 = "../.".getBytes(StandardCharsets.UTF_8);
    TokenConsumer consumer = mock(TokenConsumer.class);
    queue.push(ASCII_OTHER, 0, 4);
    tokenizer.processQueue(string1, queue, consumer);

    verify(consumer).accept(ASCII_OTHER_NGRAM, 0, 1);
    verify(consumer).accept(ASCII_OTHER_NGRAM, 1, 1);
    verify(consumer).accept(ASCII_OTHER_NGRAM, 2, 1);
    verify(consumer).accept(ASCII_OTHER_NGRAM, 3, 1);
    verify(consumer).accept(ASCII_OTHER_NGRAM, 0, 2);
    verify(consumer).accept(ASCII_OTHER_NGRAM, 1, 2);
    verify(consumer).accept(ASCII_OTHER_NGRAM, 2, 2);
    verify(consumer).accept(ASCII_OTHER_NGRAM, 0, 3);
    verify(consumer).accept(ASCII_OTHER_NGRAM, 1, 3);
    verifyNoMoreInteractions(consumer);

    // correct term - too short for tri-gram
    queue.clear();
    consumer = mock(TokenConsumer.class);
    final byte[] string4 = "...".getBytes(StandardCharsets.UTF_8);
    queue.push(ASCII_OTHER, 0, 3);
    tokenizer.processQueue(string4, queue, consumer);

    verify(consumer).accept(ASCII_OTHER_NGRAM, 0, 1);
    verify(consumer).accept(ASCII_OTHER_NGRAM, 1, 1);
    verify(consumer).accept(ASCII_OTHER_NGRAM, 2, 1);
    verify(consumer).accept(ASCII_OTHER_NGRAM, 0, 2);
    verify(consumer).accept(ASCII_OTHER_NGRAM, 1, 2);
    verifyNoMoreInteractions(consumer);

    // too short
    queue.clear();
    consumer = mock(TokenConsumer.class);
    final byte[] string2 = ".".getBytes(StandardCharsets.UTF_8);
    queue.push(ASCII_OTHER, 0, 1);
    tokenizer.processQueue(string2, queue, consumer);

    verifyNoMoreInteractions(consumer);

    // wrong signature
    queue.clear();
    consumer = mock(TokenConsumer.class);
    final byte[] string3 = "abcd".getBytes(StandardCharsets.UTF_8);
    queue.push(ASCII_ALPHA_NUM_TRI_GRAM, 0, 4);
    tokenizer.processQueue(string3, queue, consumer);

    verifyNoMoreInteractions(consumer);
  }
}