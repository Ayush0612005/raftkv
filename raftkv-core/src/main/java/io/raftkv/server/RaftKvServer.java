package io.raftkv.server;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.raftkv.kv.KvService;
import io.raftkv.kv.KvStateMachine;
import io.raftkv.raft.NodeStatus;
import io.raftkv.raft.RaftConfig;
import io.raftkv.raft.RaftNode;
import io.raftkv.storage.FileStorage;
import io.raftkv.transport.Channels;
import io.raftkv.transport.GrpcTransport;
import io.raftkv.transport.KvGrpcService;
import io.raftkv.transport.RaftGrpcService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * One RaftKV node as a process: WAL on disk, the KV state machine, and one gRPC port serving both the node-to-node
 * {@code Raft} service and the client {@code Kv} service.
 */
public final class RaftKvServer implements AutoCloseable {

    private static final Duration RPC_TIMEOUT = Duration.ofSeconds(1);
    private static final Duration COMMIT_TIMEOUT = Duration.ofSeconds(2);

    public record Options(int id, Map<Integer, String> cluster, Path dataDir, boolean fsync) {
        public Options {
            cluster = Map.copyOf(cluster);
            if (!cluster.containsKey(id)) {
                throw new IllegalArgumentException("cluster " + cluster + " does not contain this node's id " + id);
            }
        }
    }

    private final Options options;
    private final KvStateMachine stateMachine = new KvStateMachine();
    private final GrpcTransport transport;
    private final RaftNode node;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final Server server;

    private RaftKvServer(Options options) throws IOException {
        this.options = options;
        Map<Integer, String> peers = new LinkedHashMap<>(options.cluster());
        peers.remove(options.id());
        FileStorage storage = new FileStorage(options.dataDir(), options.fsync());
        this.transport = new GrpcTransport(peers, RPC_TIMEOUT);
        this.node = new RaftNode(RaftConfig.defaults(options.id(), peers.keySet().stream().sorted().toList()),
                transport, storage, stateMachine);
        this.server = Grpc.newServerBuilderForPort(port(options.cluster().get(options.id())),
                        InsecureServerCredentials.create())
                .executor(executor)
                .maxInboundMessageSize(Channels.MAX_MESSAGE_BYTES)
                .addService(new RaftGrpcService(node))
                .addService(new KvGrpcService(new KvService(node, COMMIT_TIMEOUT), node))
                .build()
                .start();
        node.start();
    }

    public static RaftKvServer start(Options options) {
        try {
            return new RaftKvServer(options);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot start node " + options.id(), e);
        }
    }

    public int id() {
        return options.id();
    }

    public NodeStatus status() {
        return node.status();
    }

    public KvStateMachine stateMachine() {
        return stateMachine;
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    private static int port(String address) {
        int colon = address.lastIndexOf(':');
        if (colon < 0) {
            throw new IllegalArgumentException("address must be host:port, got " + address);
        }
        return Integer.parseInt(address.substring(colon + 1));
    }

    @Override
    public void close() {
        server.shutdownNow();
        node.close();
        transport.close();
        try {
            server.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        executor.shutdownNow();
    }
}
