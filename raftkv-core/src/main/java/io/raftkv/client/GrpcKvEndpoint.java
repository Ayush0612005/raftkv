package io.raftkv.client;

import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.raftkv.proto.KvCommand;
import io.raftkv.proto.KvGrpc;
import io.raftkv.proto.KvResponse;
import io.raftkv.proto.StatusRequest;
import io.raftkv.proto.StatusResponse;
import io.raftkv.raft.RpcException;
import io.raftkv.transport.Channels;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/** {@link KvEndpoint} over gRPC. */
public final class GrpcKvEndpoint implements KvEndpoint {

    private static final long CALL_TIMEOUT_MS = 5_000;

    private final Map<Integer, ManagedChannel> channels = new TreeMap<>();

    public GrpcKvEndpoint(Map<Integer, String> cluster) {
        cluster.forEach((id, address) -> channels.put(id, Channels.open(address)));
    }

    @Override
    public List<Integer> serverIds() {
        return List.copyOf(channels.keySet());
    }

    @Override
    public KvResponse execute(int serverId, KvCommand command) throws RpcException {
        try {
            return KvGrpc.newBlockingStub(channel(serverId))
                    .withDeadlineAfter(CALL_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                    .execute(command);
        } catch (StatusRuntimeException e) {
            throw new RpcException("server " + serverId + ": " + e.getStatus(), e);
        }
    }

    public StatusResponse status(int serverId) throws RpcException {
        try {
            return KvGrpc.newBlockingStub(channel(serverId))
                    .withDeadlineAfter(CALL_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                    .status(StatusRequest.getDefaultInstance());
        } catch (StatusRuntimeException e) {
            throw new RpcException("server " + serverId + ": " + e.getStatus(), e);
        }
    }

    private ManagedChannel channel(int serverId) throws RpcException {
        ManagedChannel channel = channels.get(serverId);
        if (channel == null) {
            throw new RpcException("unknown server " + serverId);
        }
        return channel;
    }

    @Override
    public void close() {
        channels.values().forEach(Channels::close);
    }
}
