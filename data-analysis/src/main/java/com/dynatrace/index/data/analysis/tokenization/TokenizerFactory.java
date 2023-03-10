package com.dynatrace.index.data.analysis.tokenization;

import static com.dynatrace.index.data.analysis.tokenization.TokenQueue.TokenType.ASCII_ALPHA_NUM;

import java.util.Locale;

public final class TokenizerFactory {

  private TokenizerFactory() {
    // static helper
  }

  public static Tokenizer createTokenizer(String tokenizer) {
    final String lowerCaseTokenizer = tokenizer.toLowerCase(Locale.ROOT);
    switch (lowerCaseTokenizer) {
      case "full":
        return Tokenizers.createFull();
      case "combo":
        return Tokenizers.builder()
            .forwardAllBaseTypes()
            .addDerivedTokenizer(Tokenizers::comboTerms)
            .addDerivedTokenizer(Tokenizers::dotComboTerms)
            .build();
      case "alphanumeric":
        return Tokenizers.builder()
            .addForwardedBaseType(ASCII_ALPHA_NUM)
            .build();
      default:
        throw new IllegalArgumentException("Unhandled tokenizer config: " + lowerCaseTokenizer);
    }
  }
}
