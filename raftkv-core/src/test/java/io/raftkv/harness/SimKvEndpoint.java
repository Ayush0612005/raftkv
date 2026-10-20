package io.raftkv.harness;

import io.raftkv.client.KvEndpoint;
import io.raftkv.kv.KvService;
import io.raftkv.proto.KvCommand;
import io.raftkv.proto.KvResponse;
import io.raftkv.raft.RaftNode;
import io.raftkv.raft.RpcException;

import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;

/** Lets the real {@link io.raftkv.client.RaftKvClient} talk to a {@link SimCluster} through the faulty network. */
public final class SimKvEndpoint implements KvEndpoint {

    private static final Duration COMMIT_TIMEOUT = Duration.ofSeconds(1);

    private final SimCluster cluster;

    public SimKvEndpoint(SimCluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public List<Integer> serverIds() {
        return IntStream.range(0, cluster.size()).boxed().toList();
    }

    @Override
    public KvResponse execute(int serverId, KvCommand command) throws RpcException {
        return cluster.network().callFromClient(serverId, () -> {
            RaftNode node = cluster.node(serverId);
            return node == null ? null : new KvService(node, COMMIT_TIMEOUT).execute(command);
        });
    }
}
