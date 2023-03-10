package com.dynatrace.index.benchmark;

import static com.dynatrace.index.util.SystemUtils.dropPageCache;

import com.dynatrace.index.LogStore;
import com.dynatrace.index.LogStoreFactory;
import com.dynatrace.index.LogStoreReader;
import com.dynatrace.index.data.analysis.parser.LogLineReader;
import com.dynatrace.index.data.analysis.parser.LogParser;
import com.dynatrace.index.data.analysis.tokenization.Tokenizer;
import com.dynatrace.index.data.analysis.tokenization.TokenizerFactory;
import com.dynatrace.index.tokenization.QueryTokenSink;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Random;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Thread)
public class QueryState {

  private static final Logger LOG = LogManager.getLogger(QueryState.class);

  @Param("warp")
  public String storeType = "warp";
  @Param("logFile")
  public String logFileName;
  @Param(PathHelper.TMP_DIR_PROPERTY)
  public String rootDirName;
  @Param("20000")
  public int maxQueryTokens = 20_000;
  @Param("full")
  public String tokenizer = "full";
  @Param("2048")
  public int maxBatchCount = 2048;
  @Param("2147483647") // int max
  public int maxLineLength = Integer.MAX_VALUE;
  @Param("true")
  public boolean loadData = true;
  @Param("HOT")
  public QueryMode queryMode = QueryMode.HOT;

  @Param("8")
  public int cscSizeMB = 8;

  private final byte[] randomQueryID = new byte[16];
  private final byte[] randomQueryIP = new byte[] {
      0, 0, 0, '.', 0, 0, 0, '.', 0, 0, 0
  };
  private final Random random = new Random(81195);

  public LogStore logStore;
  private LogStoreReader reader;
  private Path indexDir;

  private QueryTokenSink tokenSink;
  private Iterator<QueryTokenSink.TokenKey> tokenIterator;


  @Setup(Level.Trial)
  public void setupIndex() throws IOException {
    LOG.info("Starting index preparation...");

    final Tokenizer logTokenizer = TokenizerFactory.createTokenizer(tokenizer);
    indexDir = PathHelper.resolvePath(rootDirName);
    logStore = LogStoreFactory.createStore(storeType, indexDir, logTokenizer, maxBatchCount, cscSizeMB);
    tokenSink = new QueryTokenSink(maxQueryTokens);

    final Path logFile = Path.of(logFileName);

    // Ingest the data
    LogLineReader.parseFile(logFile, logStore::addLogLine, maxLineLength);
    // Collect query tokens
    LogParser.parseFile(logFile, logTokenizer, tokenSink, maxLineLength, false);

    logStore.finish(new IngestFinishMetrics());

    tokenIterator = tokenSink.getQueryTokens().iterator();
    LOG.info("Finished index preparation with {} query tokens.", tokenSink.getQueryTokens().size());
  }

  @TearDown(Level.Invocation)
  public void flushCaches() {
    if (queryMode != QueryMode.HOT) {
      reader.close();
      reader = null;

      if (queryMode == QueryMode.COLD) {
        dropPageCache();
      }
    }
  }

  @TearDown(Level.Iteration)
  public void trackReaderStats(ReaderMetrics metrics) {
    if (reader == null) {
      // Acquire a new reader to enable the memory estimation in case of single shot queries
      reader = acquireReader();
    }
    metrics.trackMemoryUsage(reader.estimatedMemoryUsageBytes());
  }

  @TearDown(Level.Trial)
  public void tearDownIndex() {
    logStore.close();
    if (reader != null) {
      reader.close();
    }
  }

  public boolean isLoadData() {
    return loadData;
  }

  public LogStoreReader acquireReader() {
    if (reader == null) {
      reader = LogStoreFactory.loadReader(storeType, indexDir);
    }
    return reader;
  }

  public byte[] nextMixedQueryToken() {
    final float randomValue = random.nextFloat();
    if (randomValue < 0.5) {
      return nextIndexedToken();
    }
    return nextRandomID();
  }

  public byte[] nextIndexedToken() {
    return nextTokenEntry().bytes();
  }

  public byte[] nextRandomID() {
    // 16-byte random ascii sequence -> this is almost definitely not indexed
    for (int i = 0; i < 16; i++) {
      // Generate random sequences of [a-z] characters to make sure we query a single base token for contains queries
      randomQueryID[i] = (byte) (random.nextInt(26) + 97);
    }
    return randomQueryID;
  }

  public byte[] nextRandomIP() {
    int offset = 0;
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 3; j++) {
        randomQueryIP[offset++] = (byte) (random.nextInt(10) + 48);
      }
      offset++;
    }
    return randomQueryIP;
  }

  private QueryTokenSink.TokenKey nextTokenEntry() {
    if (!tokenIterator.hasNext()) {
      tokenIterator = tokenSink.getQueryTokens().iterator();
    }
    return tokenIterator.next();
  }

  public enum QueryMode {
    /**
     * Keep the log store reader open between queries.
     */
    HOT,

    /**
     * Open a new log store reader for each query execution.
     */
    WARM,

    /**
     * Open a new log store reader for each query and drop the OS page cache between query executions.
     */
    COLD
  }
}
