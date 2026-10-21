package io.raftkv.bench;

import io.raftkv.client.GrpcKvEndpoint;
import io.raftkv.proto.StatusResponse;
import io.raftkv.raft.RpcException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Throughput and latency versus cluster size. For each cluster size, starts that many real server processes on
 * localhost (separate JVMs, real gRPC, real WALs with fsync), sweeps the number of concurrent clients, then stops
 * the cluster. Writes {@code results.csv}, {@code results.md}, {@code environment.txt} and the SVG charts.
 *
 * <pre>
 * java -cp raftkv-bench/target/raftkv-bench.jar io.raftkv.bench.ClusterBenchmark --out docs/bench
 * </pre>
 */
@Command(name = "cluster-benchmark", mixinStandardHelpOptions = true)
public final class ClusterBenchmark implements Callable<Integer> {

    @Option(names = "--nodes", split = ",", defaultValue = "3,5")
    List<Integer> nodeCounts;

    @Option(names = "--clients", split = ",", defaultValue = "1,2,4,8,16,32,64")
    List<Integer> clientCounts;

    @Option(names = "--warmup", defaultValue = "3", description = "Seconds of load before measuring.")
    int warmupSeconds;

    @Option(names = "--duration", defaultValue = "10", description = "Seconds measured per point.")
    int durationSeconds;

    @Option(names = "--cluster-warmup", defaultValue = "15",
            description = "Seconds of unmeasured load after starting each cluster (JIT warm-up).")
    int clusterWarmupSeconds;

    @Option(names = "--value-size", defaultValue = "100")
    int valueSize;

    @Option(names = "--read-ratio", defaultValue = "0", description = "Fraction of GETs (they also go through the log).")
    double readRatio;

    @Option(names = "--keys", defaultValue = "10000")
    int keySpace;

    @Option(names = "--no-fsync", description = "Run the servers without fsync.")
    boolean noFsync;

    @Option(names = "--base-port", defaultValue = "17100")
    int basePort;

    @Option(names = "--out", defaultValue = "docs/bench")
    Path out;

    public static void main(String[] args) {
        System.exit(new CommandLine(new ClusterBenchmark()).execute(args));
    }

    @Override
    public Integer call() throws Exception {
        Files.createDirectories(out);
        List<LoadGenerator.Result> results = new ArrayList<>();
        for (int nodes : nodeCounts) {
            try (LocalCluster cluster = LocalCluster.start(nodes, basePort, !noFsync)) {
                // JIT warm-up for the server JVMs and this one: the first seconds of a fresh JVM are 3-5x slower
                LoadGenerator.run(cluster.addresses(), 16, Duration.ZERO, Duration.ofSeconds(clusterWarmupSeconds),
                        valueSize, readRatio, keySpace);
                for (int clients : clientCounts) {
                    LoadGenerator.Result r = LoadGenerator.run(cluster.addresses(), clients,
                            Duration.ofSeconds(warmupSeconds), Duration.ofSeconds(durationSeconds), valueSize,
                            readRatio, keySpace);
                    results.add(r);
                    System.out.printf("nodes=%d clients=%-3d %8.0f ops/s  p50 %6.2f ms  p99 %7.2f ms  errors %d%n",
                            r.nodes(), r.clients(), r.throughput(), r.p50Ms(), r.p99Ms(), r.errors());
                }
            }
        }
        writeResults(results);
        Charts.render(results, out);
        System.out.println("wrote " + out.toAbsolutePath());
        return 0;
    }

    private void writeResults(List<LoadGenerator.Result> results) throws IOException {
        List<String> csv = new ArrayList<>();
        csv.add(LoadGenerator.Result.CSV_HEADER);
        results.forEach(r -> csv.add(r.csv()));
        Files.write(out.resolve("results.csv"), csv);

        StringBuilder md = new StringBuilder();
        md.append("| Nodes | Clients | Throughput (ops/s) | p50 (ms) | p90 (ms) | p99 (ms) | Max (ms) |\n");
        md.append("| ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
        for (LoadGenerator.Result r : results) {
            md.append(String.format(java.util.Locale.ROOT, "| %d | %d | %,.0f | %.2f | %.2f | %.2f | %.1f |%n",
                    r.nodes(), r.clients(), r.throughput(), r.p50Ms(), r.p90Ms(), r.p99Ms(), r.maxMs()));
        }
        Files.writeString(out.resolve("results.md"), md);

        String env = String.join("\n",
                "date: " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                "os: " + System.getProperty("os.name") + " " + System.getProperty("os.version") + " "
                        + System.getProperty("os.arch"),
                "cpu: " + System.getenv().getOrDefault("PROCESSOR_IDENTIFIER", "unknown") + ", "
                        + Runtime.getRuntime().availableProcessors() + " logical CPUs",
                "java: " + System.getProperty("java.vm.name") + " " + System.getProperty("java.version"),
                "setup: all server processes and the load generator on this one machine, localhost gRPC",
                "fsync: " + !noFsync,
                "workload: " + (readRatio == 0 ? "100% PUT" : Math.round(readRatio * 100) + "% GET") + ", "
                        + valueSize + "-byte values, " + keySpace + " keys, closed loop",
                "warm-up: " + clusterWarmupSeconds + " s per cluster, then per point " + warmupSeconds
                        + " s warm-up + " + durationSeconds + " s measured",
                "");
        Files.writeString(out.resolve("environment.txt"), env);
    }

    /** N server processes on localhost, each with its own data directory. */
    static final class LocalCluster implements AutoCloseable {

        private final Map<Integer, String> addresses;
        private final List<Process> processes = new ArrayList<>();
        private final Path dataRoot;

        private LocalCluster(Map<Integer, String> addresses, Path dataRoot) {
            this.addresses = addresses;
            this.dataRoot = dataRoot;
        }

        static LocalCluster start(int nodes, int basePort, boolean fsync) throws Exception {
            Map<Integer, String> addresses = new LinkedHashMap<>();
            for (int id = 1; id <= nodes; id++) {
                addresses.put(id, "localhost:" + (basePort + id));
            }
            String spec = addresses.entrySet().stream()
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .collect(Collectors.joining(","));
            LocalCluster cluster = new LocalCluster(addresses, BenchDirs.fresh("cluster" + nodes + "-"));
            Runtime.getRuntime().addShutdownHook(new Thread(cluster::close));

            String java = ProcessHandle.current().info().command().orElse("java");
            String classpath = Path.of(ClusterBenchmark.class.getProtectionDomain().getCodeSource().getLocation()
                    .toURI()).toString();
            for (int id = 1; id <= nodes; id++) {
                List<String> command = new ArrayList<>(List.of(java, "--sun-misc-unsafe-memory-access=allow",
                        "-Xmx512m", "-cp", classpath, "io.raftkv.cli.Main", "server",
                        "--id", String.valueOf(id), "--cluster", spec,
                        "--data-dir", cluster.dataRoot.resolve("node" + id).toString()));
                if (!fsync) {
                    command.add("--no-fsync");
                }
                Path log = cluster.dataRoot.resolve("node" + id + ".log");
                cluster.processes.add(new ProcessBuilder(command)
                        .redirectErrorStream(true)
                        .redirectOutput(log.toFile())
                        .start());
            }
            cluster.awaitLeader();
            return cluster;
        }

        Map<Integer, String> addresses() {
            return addresses;
        }

        private void awaitLeader() throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            try (GrpcKvEndpoint endpoint = new GrpcKvEndpoint(addresses)) {
                while (System.nanoTime() < deadline) {
                    int up = 0;
                    boolean leader = false;
                    for (int id : addresses.keySet()) {
                        try {
                            StatusResponse s = endpoint.status(id);
                            up++;
                            leader |= s.getRole().equals("LEADER");
                        } catch (RpcException e) {
                            // not up yet
                        }
                    }
                    if (up == addresses.size() && leader) {
                        return;
                    }
                    Thread.sleep(200);
                }
            }
            throw new IllegalStateException("cluster of " + addresses.size() + " did not elect a leader in 60 s");
        }

        @Override
        public synchronized void close() {
            for (Process p : processes) {
                p.destroy();
            }
            for (Process p : processes) {
                try {
                    if (!p.waitFor(5, TimeUnit.SECONDS)) {
                        p.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            processes.clear();
            BenchDirs.delete(dataRoot);
        }
    }
}
