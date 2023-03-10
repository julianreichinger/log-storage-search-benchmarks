package com.dynatrace.index.loggrep;

public class LogGrepException extends RuntimeException {

  public LogGrepException(Throwable cause) {
    super(cause);
  }

  public LogGrepException(String message) {
    super(message);
  }
}
