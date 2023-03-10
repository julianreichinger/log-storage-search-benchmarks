package com.dynatrace.index.lucene;

import static com.dynatrace.index.storage.StorageDirectories.dataDirectory;
import static com.dynatrace.index.storage.StorageDirectories.indexDirectory;
import static com.dynatrace.index.util.FileUtils.directorySize;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import com.dynatrace.index.FinishTrace;
import com.dynatrace.index.LogStore;
import com.dynatrace.index.LogStoreBase;
import com.dynatrace.index.data.analysis.tokenization.Tokenizer;
import com.dynatrace.index.storage.DefaultBatchWriter;
import com.dynatrace.index.tokenization.BulkTokenConsumer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.BitSet;
import java.util.function.IntConsumer;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;

/**
 * Lucene based implementation of the {@link LogStore}.
 * Does NOT store any position or frequency information for tokens.
 */
@SuppressWarnings("java:S5164")
public final class LuceneLogStore extends LogStoreBase {

  private static final Logger LOG = LogManager.getLogger(LuceneLogStore.class);
  private static final String INDEX_FIELD = "tokens";
  private static final String PAYLOAD_FIELD = "payload";

  private final BitSetCollector collector;
  private final Directory directory;
  private final BatchedIndexWriter indexWriter;
  private final Path storageDirectory;

  @Nullable
  private DirectoryReader directoryReader;
  @Nullable
  private IndexSearcher indexSearcher;

  public LuceneLogStore(
      Path storageDirectory,
      DefaultBatchWriter writer,
      Directory directory,
      BatchedIndexWriter indexWriter,
      Tokenizer tokenizer,
      int maxBatchCount) {
    super(writer, dataDirectory(storageDirectory), tokenizer, indexWriter, maxBatchCount);
    this.collector = new BitSetCollector();
    this.storageDirectory = requireNonNull(storageDirectory);
    this.directory = requireNonNull(directory);
    this.indexWriter = requireNonNull(indexWriter);
  }

  @SuppressWarnings("java:S2095") // closed in close method
  public static LuceneLogStore create(Path storageDirectory, Tokenizer tokenizer, int maxBatchCount)
      throws IOException {
    MMapDirectory directory = new MMapDirectory(indexDirectory(storageDirectory));
    IndexWriter indexWriter = new IndexWriter(directory, createIndexWriterConfig());
    BatchedIndexWriter batchedWriter = new BatchedIndexWriter(indexWriter);
    DefaultBatchWriter writer = new DefaultBatchWriter(dataDirectory(storageDirectory));
    return new LuceneLogStore(storageDirectory, writer, directory, batchedWriter, tokenizer, maxBatchCount);
  }

  private static IndexWriterConfig createIndexWriterConfig() {
    IndexWriterConfig cfg = new IndexWriterConfig();
    // Flush only triggered by memory usage
    cfg.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    cfg.setRAMBufferSizeMB(32);
    // By default, Lucene uses additional background threads to perform async segment merges. This would give
    // it an unfair advantage over other implementations, so we ensure everything runs in the benchmark thread
    cfg.setMergeScheduler(new SerialMergeScheduler());
    // Improves opening times for the index
    cfg.setUseCompoundFile(true);
    return cfg;
  }

  @Override
  public void finish(FinishTrace trace) {
    try {
      trace.trackSketchMemoryUsage(indexWriter.indexWriter.ramBytesUsed());

      // Write index
      final long indexStart = System.nanoTime();
      indexWriter.finish();
      trace.trackSketchFinishTime(System.nanoTime() - indexStart);
      trace.trackSketchDiskUsage(directorySize(indexDirectory(storageDirectory)));

      directoryReader = DirectoryReader.open(directory);
      indexSearcher = new IndexSearcher(directoryReader);

      // Write data
      super.finish(trace);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  protected void locateTokenBatches(byte[] utf8Token, BitSet batches) {
    checkState(indexSearcher != null, "Index hasn't been finished yet.");
    querySearcher(indexSearcher, createTokenQuery(utf8Token), collector, batches::set);
  }

  @Override
  protected void locateContainsBatches(byte[] utf8String, BitSet batches) {
    checkState(indexSearcher != null, "Index hasn't been finished yet.");
    querySearcher(indexSearcher, createContainsQuery(utf8String), collector, batches::set);
  }

  @Override
  public long estimatedMemoryUsageBytes() {
    return indexWriter.indexWriter.ramBytesUsed();
  }

  @Override
  public void close() {
    try {
      super.close();
      if (directoryReader != null) {
        directoryReader.close();
      }
      directory.close();
    } catch (IOException e) {
      LOG.info("Failed to close lucene index.", e);
    }
  }

  static Query createTokenQuery(byte[] utf8Token) {
    return new TermQuery(new Term(INDEX_FIELD, new BytesRef(utf8Token)));
  }

  static Query createContainsQuery(byte[] utf8Token) {
    String queryTerm = new String(utf8Token, StandardCharsets.UTF_8);

    // We need to escape each character which has a special meaning in the wildcard query
    queryTerm = queryTerm.replace("*", "\\*");
    queryTerm = queryTerm.replace("?", "\\?");
    queryTerm = queryTerm.replace("\\", "\\\\");

    return new WildcardQuery(new Term(INDEX_FIELD, "*" + queryTerm + "*"));
  }

  static void querySearcher(
      IndexSearcher indexSearcher, Query query, BitSetCollector resultCollector, IntConsumer payloadConsumer) {
    try {
      indexSearcher.search(query, resultCollector);
      resultCollector.consumeResult(payloadConsumer);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static final class BatchedIndexWriter implements BulkTokenConsumer {

    private final ReusableDocument writerDoc;
    private final IndexWriter indexWriter;

    BatchedIndexWriter(IndexWriter indexWriter) {
      this.indexWriter = indexWriter;
      this.writerDoc = new ReusableDocument();
    }

    @Override
    public void acceptTokens(byte[] bytes, int[] offsets, int[] lengths, int tokenCount, int posting) {
      try {
        final Document doc = writerDoc.updateData(bytes, offsets, lengths, tokenCount, posting);
        indexWriter.addDocument(doc);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    void finish() throws IOException {
      indexWriter.flush();
      indexWriter.commit();
      indexWriter.forceMerge(1, true);
      indexWriter.deleteUnusedFiles();
      indexWriter.close();
    }
  }

  private static final class ReusableDocument {

    private final Document document;
    private final StoredField payloadField;
    private StringField[] tokenFields;
    private BytesRef[] tokenRefs;

    ReusableDocument() {
      document = new Document();
      payloadField = new StoredField(PAYLOAD_FIELD, 0);
      tokenFields = new StringField[1024];
      tokenRefs = new BytesRef[1024];
      initTokenFields(0);
    }

    public Document updateData(byte[] utf8Bytes, int[] offsets, int[] lengths, int tokenCount, int posting) {
      document.clear();
      ensureTokenArrays(tokenCount);

      for (int i = 0; i < tokenCount; i++) {
        final BytesRef ref = tokenRefs[i];
        ref.bytes = utf8Bytes;
        ref.offset = offsets[i];
        ref.length = lengths[i];

        document.add(tokenFields[i]);
      }

      payloadField.setIntValue(posting);
      document.add(payloadField);

      return document;
    }

    private void ensureTokenArrays(int size) {
      if (tokenFields.length < size) {
        int oldSize = tokenFields.length;
        int newSize = Math.max(size, oldSize * 2);
        tokenFields = Arrays.copyOf(tokenFields, newSize);
        tokenRefs = Arrays.copyOf(tokenRefs, newSize);
        initTokenFields(oldSize);
      }
    }

    private void initTokenFields(int startOffset) {
      for (int i = startOffset; i < tokenFields.length; i++) {
        tokenRefs[i] = new BytesRef();
        tokenFields[i] = new StringField(INDEX_FIELD, tokenRefs[i], Field.Store.NO);
      }
    }
  }

  static final class BitSetCollector implements Collector {

    private final BitSet result;

    BitSetCollector() {
      this.result = new BitSet();
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) {
      final LeafReader reader = context.reader();
      return new LeafCollector() {
        @Override
        public void setScorer(Scorable scorer) {
          // Nothing to do
        }

        @Override
        public void collect(int docId) throws IOException {
          Document doc = reader.document(docId);
          // Possible performance improvement: Each stored field access leads to an IO operation and a decompression
          // of the stored fields chunk. This could be improved by batching the access
          IndexableField payloadField = doc.getField(PAYLOAD_FIELD);
          result.set(payloadField.numericValue().intValue());
        }
      };
    }

    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }

    void consumeResult(IntConsumer consumer) {
      for (int i = result.nextSetBit(0); i >= 0; i = result.nextSetBit(i + 1)) {
        consumer.accept(i);
      }

      // Reset for next query
      result.clear();
    }
  }
}
