package com.dynatrace.index.benchmark;

import static java.nio.file.StandardOpenOption.READ;

import com.dynatrace.index.LogStore;
import com.dynatrace.index.LogStoreFactory;
import com.dynatrace.index.data.analysis.parser.LineSink;
import com.dynatrace.index.data.analysis.parser.LogLineReader;
import com.dynatrace.index.data.analysis.tokenization.Tokenizer;
import com.dynatrace.index.data.analysis.tokenization.TokenizerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Thread)
public class IngestState {

  @Param("warp")
  public String storeType = "warp";
  @Param("logFile")
  public String logFileName;
  @Param(PathHelper.TMP_DIR_PROPERTY)
  public String rootDirName;
  @Param("full")
  public String tokenizer = "full";
  @Param("2048")
  public int maxBatchCount = 2048;
  @Param("2147483647") // int max
  public int maxLineLength = Integer.MAX_VALUE;

  @Param("8")
  public int cscSizeMB = 8;

  private final AtomicInteger lineCount = new AtomicInteger();
  private final AtomicInteger groupCount = new AtomicInteger();

  private LogStore logStore;
  private LineSink lineSink;
  private InputStream dataIn;
  private LogLineReader logReader;

  @Setup(Level.Iteration)
  public void setupIteration(IngestMetrics metrics) throws IOException {
    Path rootDir = PathHelper.resolvePath(rootDirName);
    Tokenizer logTokenizer = TokenizerFactory.createTokenizer(tokenizer);
    logStore = LogStoreFactory.createStore(storeType, rootDir, logTokenizer, maxBatchCount, cscSizeMB);

    groupCount.set(0);
    lineCount.set(0);

    lineSink = (bytes, offset, length, posting) -> {
      logStore.addLogLine(bytes, offset, length, posting, metrics);

      groupCount.set(Math.max(groupCount.get(), posting));
      lineCount.incrementAndGet();
    };

    dataIn = Files.newInputStream(Path.of(logFileName), READ);
    logReader = LogLineReader.create(dataIn, LogLineReader.DEFAULT_BATCH_SIZE, maxLineLength);
  }

  @Setup(Level.Invocation)
  public void setupInvocation() throws IOException {
    logReader.readBatch();
  }

  @TearDown(Level.Iteration)
  public void tearDownIteration(IngestFinishMetrics metrics) throws IOException {
    try {
      logStore.finish(metrics);
      logStore.close();
    } finally {
      dataIn.close();
    }
  }

  public void indexBatch() {
    logReader.parseBatch(lineSink);
  }
}
