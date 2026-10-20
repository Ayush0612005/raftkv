package io.raftkv.cli;

import io.raftkv.client.GrpcKvEndpoint;
import io.raftkv.client.RaftKvClient;
import io.raftkv.proto.StatusResponse;
import io.raftkv.raft.RpcException;
import io.raftkv.server.RaftKvServer;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Callable;

/** {@code raftkv} command line: run a node, or talk to a cluster. */
@Command(name = "raftkv", mixinStandardHelpOptions = true, version = "raftkv 0.1.0",
        description = "Replicated key-value store on Raft.",
        subcommands = {Main.Server.class, Main.Get.class, Main.Put.class, Main.Delete.class, Main.Status.class})
public final class Main {

    public static void main(String[] args) {
        System.exit(new CommandLine(new Main()).execute(args));
    }

    static final String CLUSTER_ENV = "RAFTKV_CLUSTER";

    /** Options shared by the client commands. The cluster can also come from $RAFTKV_CLUSTER. */
    static class ClusterOption {
        @Option(names = {"-c", "--cluster"}, description = "Cluster as id=host:port,... (default: $" + CLUSTER_ENV + ")")
        String cluster;

        Map<Integer, String> resolve() {
            String spec = cluster != null ? cluster : System.getenv(CLUSTER_ENV);
            if (spec == null || spec.isBlank()) {
                throw new CommandLine.ParameterException(new CommandLine(this),
                        "no cluster: pass --cluster or set " + CLUSTER_ENV);
            }
            return ClusterSpec.parse(spec);
        }

        RaftKvClient client() {
            return new RaftKvClient(new GrpcKvEndpoint(resolve()), Duration.ofSeconds(10));
        }
    }

    @Command(name = "server", description = "Run one node of the cluster.")
    static final class Server implements Callable<Integer> {
        @Option(names = "--id", required = true, description = "This node's id (a key of --cluster).")
        int id;

        @Option(names = {"-c", "--cluster"}, required = true, description = "All nodes: id=host:port,...")
        String cluster;

        @Option(names = "--data-dir", required = true, description = "Directory for the WAL and snapshots.")
        Path dataDir;

        @Option(names = "--no-fsync", description = "Skip fsync (benchmarks only: unsafe on power loss).")
        boolean noFsync;

        @Override
        public Integer call() throws Exception {
            RaftKvServer server = RaftKvServer.start(
                    new RaftKvServer.Options(id, ClusterSpec.parse(cluster), dataDir, !noFsync));
            Runtime.getRuntime().addShutdownHook(new Thread(server::close));
            System.out.println("node " + id + " serving on " + ClusterSpec.parse(cluster).get(id));
            server.awaitTermination();
            return 0;
        }
    }

    @Command(name = "get", description = "Print the value of a key (exit code 1 if absent).")
    static final class Get implements Callable<Integer> {
        @CommandLine.Mixin
        ClusterOption cluster = new ClusterOption();

        @Parameters(index = "0")
        String key;

        @Override
        public Integer call() {
            try (RaftKvClient client = cluster.client()) {
                var value = client.get(key);
                if (value.isEmpty()) {
                    System.err.println("(not found)");
                    return 1;
                }
                System.out.println(value.get());
                return 0;
            }
        }
    }

    @Command(name = "put", description = "Set a key.")
    static final class Put implements Callable<Integer> {
        @CommandLine.Mixin
        ClusterOption cluster = new ClusterOption();

        @Parameters(index = "0")
        String key;

        @Parameters(index = "1")
        String value;

        @Override
        public Integer call() {
            try (RaftKvClient client = cluster.client()) {
                client.put(key, value);
                System.out.println("OK");
                return 0;
            }
        }
    }

    @Command(name = "delete", description = "Delete a key (exit code 1 if it did not exist).")
    static final class Delete implements Callable<Integer> {
        @CommandLine.Mixin
        ClusterOption cluster = new ClusterOption();

        @Parameters(index = "0")
        String key;

        @Override
        public Integer call() {
            try (RaftKvClient client = cluster.client()) {
                boolean existed = client.delete(key);
                System.out.println(existed ? "deleted" : "(not found)");
                return existed ? 0 : 1;
            }
        }
    }

    @Command(name = "status", description = "Show role, term and log position of every node.")
    static final class Status implements Callable<Integer> {
        @CommandLine.Mixin
        ClusterOption cluster = new ClusterOption();

        @Override
        public Integer call() {
            Map<Integer, String> nodes = cluster.resolve();
            System.out.printf("%-4s %-22s %-10s %6s %7s %9s %8s %8s%n",
                    "ID", "ADDRESS", "ROLE", "TERM", "LEADER", "LAST_LOG", "COMMIT", "APPLIED");
            try (GrpcKvEndpoint endpoint = new GrpcKvEndpoint(nodes)) {
                for (var node : nodes.entrySet()) {
                    try {
                        StatusResponse s = endpoint.status(node.getKey());
                        System.out.printf("%-4d %-22s %-10s %6d %7d %9d %8d %8d%n", s.getId(), node.getValue(),
                                s.getRole(), s.getTerm(), s.getLeaderId(), s.getLastLogIndex(), s.getCommitIndex(),
                                s.getLastApplied());
                    } catch (RpcException e) {
                        System.out.printf("%-4d %-22s %-10s%n", node.getKey(), node.getValue(), "DOWN");
                    }
                }
            }
            return 0;
        }
    }
}
