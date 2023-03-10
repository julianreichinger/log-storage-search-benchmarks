package com.dynatrace.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.dynatrace.index.data.analysis.tokenization.Tokenizers;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class LogStoreTest {

  @ParameterizedTest
  @ValueSource(strings = {"csc", "csc-bf", "lucene", "scan"})
  void writeReadLogLines(String storeType, @TempDir Path tempDir) throws IOException {
    testWriteReadLogLines(
        () -> LogStoreFactory.createStore(storeType, tempDir, Tokenizers.createFull(), 2048, 8),
        () -> LogStoreFactory.loadReader(storeType, tempDir));
  }

  private void testWriteReadLogLines(
      Supplier<LogStore> storeSupplier,
      Supplier<LogStoreReader> readerSupplier) throws IOException {

    final byte[] log1 = "Look at my horse, my horse is amazing.".getBytes(StandardCharsets.UTF_8);
    final byte[] log2 = "Give it a lick!".getBytes(StandardCharsets.UTF_8);
    final byte[] log3 = "Mmm! It tastes just like raisins.".getBytes(StandardCharsets.UTF_8);
    final byte[] log4 = "Get on my horse! I'll take you 'round the universe and all the other places, too."
        .getBytes(StandardCharsets.UTF_8);

    final LogStore logStore = storeSupplier.get();
    logStore.addLogLine(log1, 0, log1.length, 0);
    logStore.addLogLine(log2, 0, log2.length, 1);
    logStore.addLogLine(log3, 0, log3.length, 4);
    logStore.addLogLine(log4, 0, log4.length, 0);
    logStore.addLogLine(log2, 0, log2.length, 1);

    logStore.finish(mock(FinishTrace.class));
    checkLogs(logStore);

    logStore.close();

    final LogStoreReader reader = readerSupplier.get();
    checkLogs(reader);

    reader.close();
  }

  private void checkLogs(LogStoreReader reader) {
    assertTokenLogs(reader, "horse",
        "Look at my horse, my horse is amazing.",
        "Get on my horse! I'll take you 'round the universe and all the other places, too.");

    assertContainsLogs(reader, "ivers",
        "Get on my horse! I'll take you 'round the universe and all the other places, too.");

    assertTokenLogs(reader, "lick",
        "Give it a lick!",
        "Give it a lick!");
  }

  private void assertTokenLogs(LogStoreReader reader, String query, String... expectedLines) {
    final List<String> logLines = new ArrayList<>();
    reader.queryToken(
        query.getBytes(StandardCharsets.UTF_8),
        (bytes, offset, length) -> logLines.add(new String(bytes, offset, length, StandardCharsets.UTF_8)),
        mock(QueryTrace.class),
        true);
    assertThat(logLines).containsExactlyInAnyOrder(expectedLines);
  }

  private void assertContainsLogs(LogStoreReader reader, String query, String... expectedLines) {
    final List<String> logLines = new ArrayList<>();
    reader.queryContains(
        query.getBytes(StandardCharsets.UTF_8),
        (bytes, offset, length) -> logLines.add(new String(bytes, offset, length, StandardCharsets.UTF_8)),
        mock(QueryTrace.class),
        true);
    assertThat(logLines).containsExactlyInAnyOrder(expectedLines);
  }
}
