package io.raftkv.client;

import io.raftkv.proto.KvCommand;
import io.raftkv.proto.KvResponse;
import io.raftkv.raft.RpcException;

import java.util.List;

/** How a {@link RaftKvClient} reaches the servers: gRPC in production, the simulated network in tests. */
public interface KvEndpoint extends AutoCloseable {

    List<Integer> serverIds();

    KvResponse execute(int serverId, KvCommand command) throws RpcException;

    @Override
    default void close() {
    }
}
