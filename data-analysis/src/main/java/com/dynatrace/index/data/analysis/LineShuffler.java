package com.dynatrace.index.data.analysis;

import com.dynatrace.index.data.analysis.cli.CliOptions;
import com.dynatrace.index.data.analysis.parser.BatchBuffer;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;

/**
 * Shuffles the lines of a file and writes the shuffled lines to a new file.
 *
 * <p>Configuration parameters:
 * <ul>
 *   <li>"in" (required): input file
 *   <li>"out" (required): output file
 * </ul>
 */
public final class LineShuffler {

  private static final int BUFFER_SIZE = 16 * 1024 * 1024;

  private static final String INPUT_FILE_OPTION = "in";
  private static final String OUTPUT_FILE_OPTION = "out";

  private LineShuffler() {
    // static helper
  }

  public static void execute(CliOptions cliOptions) throws IOException {
    final Path inputFile = cliOptions.getAsPath(INPUT_FILE_OPTION);
    final Path outputFile = cliOptions.getAsPath(OUTPUT_FILE_OPTION);

    shuffleLines(inputFile, outputFile);
  }

  static void shuffleLines(Path inputFile, Path outputFile) throws IOException {
    try (FileInputStream inputStream = new FileInputStream(inputFile.toFile());
         FileOutputStream outputStream = new FileOutputStream(outputFile.toFile())) {

      final FileChannel inputChannel = inputStream.getChannel();
      final FileChannel outputChannel = outputStream.getChannel();

      final long[] lineOffsets = findLineOffsets(inputStream);
      final int[] shuffledLines = IntStream.range(0, lineOffsets.length - 1).toArray();
      shuffle(shuffledLines);

      for (int line : shuffledLines) {
        final long lineOffset = lineOffsets[line];
        final long lineLength = lineOffsets[line + 1] - lineOffset;

        inputChannel.transferTo(lineOffset, lineLength, outputChannel);
      }
    }
  }

  private static long[] findLineOffsets(InputStream inputStream) throws IOException {
    final BatchBuffer batchBuffer = new BatchBuffer(inputStream, BUFFER_SIZE);
    final LongArrayList offsets = new LongArrayList();
    // First line always has offset 0
    offsets.add(0);

    long filePosition = 0;
    int batchPosition = 0;
    while (!batchBuffer.reachedEof() && batchBuffer.readBatch(batchPosition) > 0) {
      batchPosition = 0;

      int lineEnd;
      while ((lineEnd = batchBuffer.indexOf(batchPosition, (byte) '\n')) >= 0) {
        batchPosition = lineEnd + 1;
        offsets.add(filePosition + batchPosition);
      }

      filePosition += batchPosition;
    }

    // Last position is the file length
    offsets.add(filePosition);

    return offsets.toArray();
  }

  private static void shuffle(int[] array) {
    final ThreadLocalRandom random = ThreadLocalRandom.current();
    for (int i = array.length; i > 1; i--) {
      swap(array, i - 1, random.nextInt(i));
    }
  }

  private static void swap(int[] array, int i, int j) {
    int temp = array[i];
    array[i] = array[j];
    array[j] = temp;
  }
}
