package com.dynatrace.index.data.analysis.cli;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Utility class for the processing of command line parameters.
 */
public final class CliOptions {

  private static final String SEPARATOR = "=";

  private final Map<String, String> options;

  public CliOptions(Map<String, String> options) {
    this.options = requireNonNull(options);
  }

  public static CliOptions parse(String[] arguments) {
    Map<String, String> options = new HashMap<>();
    for (String argument : arguments) {
      final String[] parts = argument.split(SEPARATOR);
      checkArgument(parts.length == 2, "Illegal argument %s.", argument);

      options.put(parts[0], parts[1]);
    }

    return new CliOptions(options);
  }

  public String getAsStringOrDefault(String key, @Nullable String defaultValue) {
    final String value = options.get(key);
    return value == null ? defaultValue : value;
  }

  public Path getAsPath(String key) {
    return Path.of(getRequiredValue(key));
  }

  @Nullable
  public Path getAsPathOrDefault(String key, @Nullable Path defaultValue) {
    final String value = options.get(key);
    return value == null ? defaultValue : Path.of(value);
  }

  public int getAsIntOrDefault(String key, int defaultValue) {
    final String value = options.get(key);
    return value == null ? defaultValue : Integer.parseInt(value);
  }

  public boolean getAsBooleanOrDefault(String key, boolean defaultValue) {
    final String value = options.get(key);
    return value == null ? defaultValue : Boolean.parseBoolean(value);
  }

  private String getRequiredValue(String key) {
    final String value = options.get(key);
    checkNotNull(value, "Option '%s' has not been specified.", key);
    return value;
  }
}
