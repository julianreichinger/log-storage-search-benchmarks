package com.dynatrace.index.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BatchWriterTest {

  @Test
  void writeReadBatches(@TempDir Path tempDir) {
    final BatchWriter batchWriter = new DefaultBatchWriter(tempDir);

    addLog(batchWriter, 0, "log line 0/1");
    addLog(batchWriter, 1, "log line 1/1");
    addLog(batchWriter, 0, "log line 0/2");
    addLog(batchWriter, 4, "log line 2/1");
    addLog(batchWriter, 0, "log line 0/3");

    batchWriter.flush();

    assertLogs(batchWriter, 0, "log line 0/1", "log line 0/2", "log line 0/3");
    assertLogs(batchWriter, 1, "log line 1/1");
    assertLogs(batchWriter, 2);
    assertLogs(batchWriter, 4, "log line 2/1");

    batchWriter.close();

    final DefaultBatchReader reader = DefaultBatchReader.create(tempDir);
    assertLogs(reader, 0, "log line 0/1", "log line 0/2", "log line 0/3");
    assertLogs(reader, 1, "log line 1/1");
    assertLogs(reader, 2);
    assertLogs(reader, 4, "log line 2/1");

    reader.close();
  }

  private void addLog(BatchWriter batchWriter, int posting, String logLine) {
    final byte[] bytes = logLine.getBytes(StandardCharsets.UTF_8);
    batchWriter.addLogLine(bytes, 0, bytes.length, posting);
  }

  private void assertLogs(BatchReader reader, int posting, String... expectedLines) {
    List<String> logLines = new ArrayList<>();
    reader.readBatch(posting, (bytes, offset, length) ->
        logLines.add(new String(bytes, offset, length, StandardCharsets.UTF_8)));
    assertThat(logLines).containsExactly(expectedLines);
  }
}