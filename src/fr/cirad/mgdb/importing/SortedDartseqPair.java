package fr.cirad.mgdb.importing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import fr.cirad.tools.ProgressIndicator;

/**
 * Sorts a DArTseq genotype file and its read-counts file by CloneID on disk, so
 * that both can then be streamed side by side, one clone at a time.
 *
 * <p>Each file is read in chunks bounded by available heap; each chunk is sorted
 * in memory and written as a temporary run, and the runs are merged into a sorted
 * copy of the file (header first, banner lines dropped). The sort is stable: lines
 * of a clone keep their original order. Both files are sorted concurrently, and
 * progress is reported on the caller's current step.</p>
 */
public class SortedDartseqPair implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(SortedDartseqPair.class);

    private static final long MIN_CHUNK_BYTES = 8L * 1024 * 1024;

    private final File sortedGenotypes;
    private final File sortedCounts;

    private SortedDartseqPair(File sortedGenotypes, File sortedCounts) {
        this.sortedGenotypes = sortedGenotypes;
        this.sortedCounts = sortedCounts;
    }

    /**
     * Sorts the given files by CloneID.
     *
     * @param genotypeFile        the .dart file
     * @param genotypeCloneIdCol  0-based index of the CloneID column in the .dart file
     * @param countsFile          the .dartcounts file
     * @param countsCloneIdCol    0-based index of the CloneID column in the counts file
     * @param progress            progress indicator (may be null)
     */
    public static SortedDartseqPair sort(File genotypeFile, int genotypeCloneIdCol, File countsFile, int countsCloneIdCol,
                                         ProgressIndicator progress) throws Exception {
        ProgressTracker tracker = new ProgressTracker(progress, 2 * (genotypeFile.length() + countsFile.length()));  // each byte is read twice: chunking, then merging
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<File> countsFuture = pool.submit(() -> sortFile(countsFile, countsCloneIdCol, tracker));
            File sortedGenotypes;
            try {
                sortedGenotypes = sortFile(genotypeFile, genotypeCloneIdCol, tracker);
            } catch (Exception e) {
                try { delete(countsFuture.get()); } catch (Exception ignored) { }
                throw e;
            }
            try {
            	progress.setPercentageEnabled(false);
                return new SortedDartseqPair(sortedGenotypes, countsFuture.get());
            } catch (ExecutionException e) {
                delete(sortedGenotypes);
                throw e.getCause() instanceof Exception ? (Exception) e.getCause() : e;
            }
        } finally {
            pool.shutdown();
        }
    }

    /** Returns a sorted copy of the given file, with its header line first. */
    private static File sortFile(File source, int cloneIdCol, ProgressTracker tracker) throws IOException {
        long start = System.currentTimeMillis();
        long chunkBytes = Math.max(MIN_CHUNK_BYTES, allocatableMemory() / 8);    // one chunk per file may be in memory at once
        List<File> runs = new ArrayList<>();
        File sorted = null;
        try (BufferedReader br = reader(source)) {
            String header = br.readLine();
            while (header != null && header.startsWith("*,"))
                header = br.readLine();
            if (header == null)
                throw new IOException("Empty file: " + source.getName());

            // Sorted runs, each holding as many lines as fit in the chunk budget
            List<String[]> chunk = new ArrayList<>();   // {CloneID, line}
            long chunkSize = 0;
            for (String line = br.readLine(); line != null; line = br.readLine()) {
                chunk.add(new String[] {extractField(line, cloneIdCol), line});
                chunkSize += line.length() + 64;    // rough heap footprint of a line
                tracker.add(line.length() + 1);
                if (chunkSize >= chunkBytes) {
                    runs.add(writeRun(chunk));
                    chunk.clear();
                    chunkSize = 0;
                }
            }
            if (!chunk.isEmpty())
                runs.add(writeRun(chunk));

            sorted = File.createTempFile("dartseq-sorted-", ".csv");
            merge(runs, header, cloneIdCol, sorted, tracker);
            LOG.debug("Sorted " + source.getName() + " (" + runs.size() + " runs) in " + (System.currentTimeMillis() - start) + "ms");
            return sorted;
        } catch (IOException | RuntimeException e) {
            delete(sorted);
            throw e;
        } finally {
            for (File run : runs)
                delete(run);
        }
    }

    private static File writeRun(List<String[]> chunk) throws IOException {
        chunk.sort(Comparator.comparing(entry -> entry[0]));    // stable
        File run = File.createTempFile("dartseq-run-", ".csv");
        try (BufferedWriter w = writer(run)) {
            for (String[] entry : chunk) {
                w.write(entry[1]);
                w.write('\n');
            }
        } catch (IOException e) {
            delete(run);
            throw e;
        }
        return run;
    }

    /**
     * k-way merge of sorted runs. Ties go to the earlier run (runs follow file order),
     * keeping the sort stable, so the two lines of a 2-row marker remain adjacent.
     */
    private static void merge(List<File> runs, String header, int cloneIdCol, File target, ProgressTracker tracker) throws IOException {
        List<RunReader> readers = new ArrayList<>();
        PriorityQueue<RunReader> heap = new PriorityQueue<>(Comparator.<RunReader, String>comparing(r -> r.key).thenComparingInt(r -> r.index));
        try (BufferedWriter w = writer(target)) {
            w.write(header);
            w.write('\n');
            for (File run : runs) {
                RunReader r = new RunReader(run, readers.size(), cloneIdCol);
                readers.add(r);
                if (r.advance())
                    heap.add(r);
            }
            while (!heap.isEmpty()) {
                RunReader r = heap.poll();
                w.write(r.line);
                w.write('\n');
                tracker.add(r.line.length() + 1);
                if (r.advance())
                    heap.add(r);
            }
        } finally {
            for (RunReader r : readers)
                r.reader.close();
        }
    }

    private static final class RunReader {
        final BufferedReader reader;
        final int index, cloneIdCol;
        String line, key;

        RunReader(File run, int index, int cloneIdCol) throws IOException {
            this.reader = reader(run);
            this.index = index;
            this.cloneIdCol = cloneIdCol;
        }

        boolean advance() throws IOException {
            line = reader.readLine();
            key = line == null ? null : extractField(line, cloneIdCol);
            return line != null;
        }
    }

    /** Reports the share of bytes processed on the caller's current step, and stops the sort if the import is aborted. */
    private static final class ProgressTracker {
        final ProgressIndicator progress;
        final long totalBytes;
        final AtomicLong processedBytes = new AtomicLong();

        ProgressTracker(ProgressIndicator progress, long totalBytes) {
        	progress.setPercentageEnabled(true);
            this.progress = progress;
            this.totalBytes = Math.max(1, totalBytes);
        }

        void add(long bytes) throws IOException {
            long processed = processedBytes.addAndGet(bytes);
            if (progress == null || processed * 100 / totalBytes == (processed - bytes) * 100 / totalBytes)
                return;     // only act when the percentage changes
            if (progress.isAborted())
                throw new IOException("Aborted by user");
            progress.setCurrentStepProgress((int) Math.min(100, processed * 100 / totalBytes));
        }
    }

    private static long allocatableMemory() {
        Runtime rt = Runtime.getRuntime();
        long maxHeap = rt.maxMemory();
        return maxHeap - (rt.totalMemory() - rt.freeMemory()) - maxHeap / 4;
    }

    private static BufferedReader reader(File file) throws IOException {
        return new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
    }

    private static BufferedWriter writer(File file) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8));
    }

    private static void delete(File f) {
        if (f != null && f.exists() && !f.delete())
            f.deleteOnExit();
    }

    /** Returns the (trimmed) value of the given column in a CSV line, or "" if absent. */
    static String extractField(String line, int col) {
        int start = 0, currentCol = 0;
        for (int i = 0; i <= line.length(); i++)
            if (i == line.length() || line.charAt(i) == ',') {
                if (currentCol++ == col)
                    return line.substring(start, i).trim();
                start = i + 1;
            }
        return "";
    }

    /** The sorted genotype file, starting with its header line. */
    public File getSortedGenotypeFile() {
        return sortedGenotypes;
    }

    /** Iterates over the data lines of the sorted counts file. */
    public LineIterator counts() throws IOException {
        return new LineIterator(sortedCounts);
    }

    @Override
    public void close() {
        delete(sortedGenotypes);
        delete(sortedCounts);
    }

    public static class LineIterator implements AutoCloseable {
        private final BufferedReader reader;
        private String nextLine;

        LineIterator(File file) throws IOException {
            reader = reader(file);
            reader.readLine();  // header
            nextLine = reader.readLine();
        }

        public boolean hasNext() {
            return nextLine != null;
        }

        public String peek() {
            return nextLine;
        }

        public String next() throws IOException {
            String current = nextLine;
            nextLine = reader.readLine();
            return current;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }
}
