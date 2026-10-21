package io.raftkv.bench;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * Scratch directories for benchmarks. They live under {@code raftkv.bench.dir} (default {@code bench/work}, relative
 * to where the benchmark is started) rather than the system temp dir, so the disk being measured is the project's
 * disk, and nothing lands on another drive.
 */
final class BenchDirs {

    private BenchDirs() {
    }

    static Path root() {
        return Path.of(System.getProperty("raftkv.bench.dir", "bench/work")).toAbsolutePath();
    }

    static Path fresh(String prefix) {
        try {
            Files.createDirectories(root());
            return Files.createTempDirectory(root(), prefix);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static void delete(Path dir) {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(dir)) {
            paths.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    // best effort: Windows may hold a handle a little longer
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
