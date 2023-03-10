package com.dynatrace.index.lucene;

import static com.dynatrace.index.lucene.LuceneLogStore.createContainsQuery;
import static com.dynatrace.index.lucene.LuceneLogStore.createTokenQuery;
import static com.dynatrace.index.lucene.LuceneLogStore.querySearcher;
import static com.dynatrace.index.storage.StorageDirectories.dataDirectory;
import static com.dynatrace.index.storage.StorageDirectories.indexDirectory;

import com.dynatrace.index.LogStoreReader;
import com.dynatrace.index.LogStoreReaderBase;
import com.dynatrace.index.lucene.LuceneLogStore.BitSetCollector;
import com.dynatrace.index.storage.BatchReader;
import com.dynatrace.index.storage.DefaultBatchReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.BitSet;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;

/**
 * Lucene based implementation of the {@link LogStoreReader}.
 */
public final class LuceneLogStoreReader extends LogStoreReaderBase {

  private final Directory directory;
  private final DirectoryReader directoryReader;
  private final IndexSearcher indexSearcher;
  private final BitSetCollector collector;

  LuceneLogStoreReader(
      Directory directory, DirectoryReader directoryReader, IndexSearcher indexSearcher, BatchReader reader) {
    super(reader);
    this.directory = directory;
    this.directoryReader = directoryReader;
    this.indexSearcher = indexSearcher;
    this.collector = new BitSetCollector();
  }

  @Override
  public long estimatedMemoryUsageBytes() {
    return 0;
  }

  @Override
  public void close() {
    try {
      super.close();
      directoryReader.close();
      directory.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  protected void locateTokenBatches(byte[] utf8Token, BitSet batches) {
    querySearcher(indexSearcher, createTokenQuery(utf8Token), collector, batches::set);
  }

  @Override
  protected void locateContainsBatches(byte[] utf8String, BitSet batches) {
    querySearcher(indexSearcher, createContainsQuery(utf8String), collector, batches::set);
  }

  public static LuceneLogStoreReader loadFromDisk(Path storageDirectory) throws IOException {
    final Path indexDir = indexDirectory(storageDirectory);
    final Path dataDir = dataDirectory(storageDirectory);
    final BatchReader reader = DefaultBatchReader.create(dataDir);

    MMapDirectory directory = new MMapDirectory(indexDir);
    DirectoryReader directoryReader = DirectoryReader.open(directory);
    IndexSearcher indexSearcher = new IndexSearcher(directoryReader);

    return new LuceneLogStoreReader(directory, directoryReader, indexSearcher, reader);
  }
}
