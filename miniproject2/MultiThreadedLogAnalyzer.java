package miniproject2;

import java.io.*;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;


public class MultiThreadedLogAnalyzer {

    // Use ConcurrentHashMap with LongAdder for efficient concurrent counting
    private final ConcurrentHashMap<String, LongAdder> globalCounts = new ConcurrentHashMap<>();
    private final List<Path> filesToProcess = new ArrayList<>();
    private final Path logsDir;
    private final Path resultFile = Paths.get("analysis_result.txt");

    public MultiThreadedLogAnalyzer(Path logsDir) {
        this.logsDir = logsDir;
    }

    // Find .txt files under the folder (non-recursive by default; change if you want recursion)
    private void discoverFiles() throws IOException {
        try (Stream<Path> stream = Files.walk(logsDir, 1)) {
            stream.filter(Files::isRegularFile)
                    .filter(p -> p.toString().toLowerCase().endsWith(".txt"))
                    .forEach(filesToProcess::add);
        }
    }


    private Map<String, Long> analyzeFileSequential(Path file) throws IOException {
        Map<String, Long> localCounts = new HashMap<>();
        try (Stream<String> lines = Files.lines(file, StandardCharsets.UTF_8)) {
            lines.flatMap(line -> Arrays.stream(line.split("\\W+"))) // split by non-word chars
                    .map(String::toLowerCase)
                    .filter(s -> !s.isBlank())
                    .forEach(token -> localCounts.merge(token, 1L, Long::sum));
        }
        return localCounts;
    }


    private void mergeIntoGlobal(Map<String, Long> localCounts) {
        localCounts.forEach((k, v) -> {
            globalCounts.computeIfAbsent(k, key -> new LongAdder()).add(v);
        });
    }


    private long runSequential() throws IOException {
        globalCounts.clear();
        long start = System.nanoTime();
        for (Path file : filesToProcess) {
            System.out.println("[SEQUENTIAL] Processing: " + file.getFileName());
            Map<String, Long> local = analyzeFileSequential(file);
            mergeIntoGlobal(local);
        }
        long end = System.nanoTime();
        return TimeUnit.NANOSECONDS.toMillis(end - start);
    }


    private long runConcurrent(int numThreads) throws Exception {
        globalCounts.clear();


        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads);

        try {
            List<Callable<Map<String, Long>>> tasks = new ArrayList<>();
            for (Path file : filesToProcess) {
                tasks.add(new LogFileTask(file));
            }

            long start = System.nanoTime();


            List<Future<Map<String, Long>>> futures = executor.invokeAll(tasks);


            System.out.println("[CONCURRENT] Thread pool size: " + executor.getPoolSize() +
                    ", Active threads: " + executor.getActiveCount() +
                    ", Task count: " + executor.getTaskCount());


            for (Future<Map<String, Long>> f : futures) {
                Map<String, Long> local = f.get(); // blocking get
                mergeIntoGlobal(local);
            }

            long end = System.nanoTime();
            return TimeUnit.NANOSECONDS.toMillis(end - start);
        } finally {
            executor.shutdownNow();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                System.out.println("[CONCURRENT] Executor did not terminate within timeout.");
            }
        }
    }

    // Write a human-readable result file
    private void writeResultsToFile(long seqTimeMs, long concTimeMs, int numThreads) {
        try (BufferedWriter bw = Files.newBufferedWriter(resultFile, StandardCharsets.UTF_8)) {
            bw.write("Multi-Threaded Log Analyzer - Results\n");
            bw.write("Logs directory: " + logsDir.toAbsolutePath().toString() + "\n");
            bw.write("Files processed: " + filesToProcess.size() + "\n");
            bw.write("Threads used (concurrent): " + numThreads + "\n");
            bw.write("Execution time (sequential): " + seqTimeMs + " ms\n");
            bw.write("Execution time (concurrent): " + concTimeMs + " ms\n");
            bw.write("Speedup (seq / conc): " + String.format("%.2f", (seqTimeMs > 0 ? (double) seqTimeMs / concTimeMs : 0.0)) + "\n");
            bw.write("\nTop 50 keywords (by count):\n");

            // Sort globalCounts by value descending
            List<Map.Entry<String, Long>> sorted = new ArrayList<>();
            globalCounts.forEach((k, v) -> sorted.add(Map.entry(k, v.longValue())));
            sorted.sort((a, b) -> Long.compare(b.getValue(), a.getValue()));

            int limit = Math.min(sorted.size(), 50);
            for (int i = 0; i < limit; i++) {
                Map.Entry<String, Long> e = sorted.get(i);
                bw.write(String.format("%3d. %-20s : %d\n", i + 1, e.getKey(), e.getValue()));
            }

            bw.flush();
            System.out.println("[IO] Results written to " + resultFile.toAbsolutePath());

        } catch (IOException e) {
            System.err.println("Error writing results file: " + e.getMessage());
        }
    }

    // Helper: pretty print top N tokens to console
    private void printTopToConsole(int topN) {
        System.out.println("\n--- TOP " + topN + " KEYWORDS ---");
        List<Map.Entry<String, Long>> sorted = new ArrayList<>();
        globalCounts.forEach((k, v) -> sorted.add(Map.entry(k, v.longValue())));
        sorted.sort((a, b) -> Long.compare(b.getValue(), a.getValue()));
        int limit = Math.min(sorted.size(), topN);
        for (int i = 0; i < limit; i++) {
            Map.Entry<String, Long> e = sorted.get(i);
            System.out.printf("%3d. %-20s : %d%n", i + 1, e.getKey(), e.getValue());
        }
    }

    // Task that analyzes a file and returns a Map<String,Long> of local counts
    private static class LogFileTask implements Callable<Map<String, Long>> {
        private final Path file;

        LogFileTask(Path file) {
            this.file = file;
        }

        @Override
        public Map<String, Long> call() throws Exception {
            // For monitoring, print which thread is processing which file
            String threadName = Thread.currentThread().getName();
            System.out.println("[THREAD] " + threadName + " -> processing " + file.getFileName());

            Map<String, Long> localCounts = new HashMap<>();
            try (Stream<String> lines = Files.lines(file, StandardCharsets.UTF_8)) {
                lines.flatMap(line -> Arrays.stream(line.split("\\W+")))
                        .map(String::toLowerCase)
                        .filter(s -> !s.isBlank())
                        .forEach(token -> localCounts.merge(token, 1L, Long::sum));
            } catch (IOException e) {
                System.err.println("[THREAD] " + threadName + " -> error reading " + file.getFileName() + " : " + e.getMessage());
            }
            return localCounts;
        }
    }

    // Entry point
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java MultiThreadedLogAnalyzer <logs_folder_path> [numThreads]");
            System.exit(1);
        }

        Path logsDir = Paths.get(args[0]);
        int numThreads = (args.length >= 2) ? Integer.parseInt(args[1]) : Math.max(1, Runtime.getRuntime().availableProcessors());

        if (!Files.isDirectory(logsDir)) {
            System.err.println("Provided path is not a directory: " + logsDir);
            System.exit(1);
        }

        MultiThreadedLogAnalyzer analyzer = new MultiThreadedLogAnalyzer(logsDir);

        try {
            analyzer.discoverFiles();
            System.out.println("Discovered " + analyzer.filesToProcess.size() + " .txt files to process.");

            if (analyzer.filesToProcess.isEmpty()) {
                System.out.println("No log files found. Exiting.");
                System.exit(0);
            }

            // Run sequential
            System.out.println("\n=== Running sequential analysis ===");
            long seqMs = analyzer.runSequential();
            System.out.println("Sequential run time: " + seqMs + " ms");

            // Run concurrent
            System.out.println("\n=== Running concurrent analysis with " + numThreads + " threads ===");
            long concMs = analyzer.runConcurrent(numThreads);
            System.out.println("Concurrent run time: " + concMs + " ms");

            // Print results and write to file
            analyzer.printTopToConsole(20);
            analyzer.writeResultsToFile(seqMs, concMs, numThreads);

            System.out.println("\n=== Done ===");

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

