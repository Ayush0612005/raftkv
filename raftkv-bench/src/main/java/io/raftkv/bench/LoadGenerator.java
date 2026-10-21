package io.raftkv.bench;

import io.raftkv.client.GrpcKvEndpoint;
import io.raftkv.client.KvTimeoutException;
import io.raftkv.client.RaftKvClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Closed-loop load: {@code clients} independent clients, each with one operation in flight, as fast as the cluster
 * answers. Every client uses the real client library over gRPC. Latency is measured per operation from the
 * client's side (including retries and leader redirects); only operations that start after the warm-up and finish
 * before the end of the window count.
 */
public final class LoadGenerator {

    private LoadGenerator() {
    }

    public record Result(int nodes, int clients, long ops, double seconds, double throughput, double p50Ms,
                         double p90Ms, double p99Ms, double maxMs, long errors) {

        static final String CSV_HEADER = "nodes,clients,ops,seconds,throughput_ops_s,p50_ms,p90_ms,p99_ms,max_ms,errors";

        String csv() {
            return String.format(java.util.Locale.ROOT, "%d,%d,%d,%.1f,%.1f,%.3f,%.3f,%.3f,%.3f,%d",
                    nodes, clients, ops, seconds, throughput, p50Ms, p90Ms, p99Ms, maxMs, errors);
        }
    }

    public static Result run(Map<Integer, String> cluster, int clients, Duration warmup, Duration measure,
                             int valueSize, double readRatio, int keySpace) throws InterruptedException {
        long start = System.nanoTime();
        long measureFrom = start + warmup.toNanos();
        long measureTo = measureFrom + measure.toNanos();
        String value = "v".repeat(valueSize);
        long[][] latencies = new long[clients][];
        int[] counts = new int[clients];
        AtomicLong errors = new AtomicLong();

        List<Thread> threads = new ArrayList<>();
        for (int c = 0; c < clients; c++) {
            int me = c;
            threads.add(Thread.ofVirtual().name("load-" + c).start(() -> {
                long[] mine = new long[4096];
                int n = 0;
                ThreadLocalRandom random = ThreadLocalRandom.current();
                try (RaftKvClient client = new RaftKvClient(new GrpcKvEndpoint(cluster), Duration.ofSeconds(10))) {
                    while (System.nanoTime() < measureTo) {
                        String key = "key-" + random.nextInt(keySpace);
                        long t0 = System.nanoTime();
                        try {
                            if (random.nextDouble() < readRatio) {
                                client.get(key);
                            } else {
                                client.put(key, value);
                            }
                        } catch (KvTimeoutException e) {
                            errors.incrementAndGet();
                            continue;
                        }
                        long t1 = System.nanoTime();
                        if (t0 >= measureFrom && t1 <= measureTo) {
                            if (n == mine.length) {
                                mine = Arrays.copyOf(mine, n * 2);
                            }
                            mine[n++] = t1 - t0;
                        }
                    }
                }
                latencies[me] = mine;
                counts[me] = n;
            }));
        }
        for (Thread t : threads) {
            t.join();
        }

        int total = Arrays.stream(counts).sum();
        long[] all = new long[total];
        int pos = 0;
        for (int c = 0; c < clients; c++) {
            System.arraycopy(latencies[c], 0, all, pos, counts[c]);
            pos += counts[c];
        }
        Arrays.sort(all);
        double seconds = measure.toNanos() / 1e9;
        return new Result(cluster.size(), clients, total, seconds, total / seconds,
                percentileMs(all, 0.50), percentileMs(all, 0.90), percentileMs(all, 0.99),
                total == 0 ? 0 : all[total - 1] / 1e6, errors.get());
    }

    private static double percentileMs(long[] sorted, double p) {
        if (sorted.length == 0) {
            return 0;
        }
        int i = (int) Math.ceil(p * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(sorted.length - 1, i))] / 1e6;
    }
}
