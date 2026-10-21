package io.raftkv.transport;

import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.raftkv.proto.AppendEntriesRequest;
import io.raftkv.proto.AppendEntriesResponse;
import io.raftkv.proto.InstallSnapshotRequest;
import io.raftkv.proto.InstallSnapshotResponse;
import io.raftkv.proto.RaftGrpc;
import io.raftkv.proto.RequestVoteRequest;
import io.raftkv.proto.RequestVoteResponse;
import io.raftkv.raft.RaftTransport;
import io.raftkv.raft.RpcException;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Node-to-node RPCs over gRPC: one long-lived channel per peer, a deadline on every call. */
public final class GrpcTransport implements RaftTransport, AutoCloseable {

    private final Map<Integer, ManagedChannel> channels = new HashMap<>();
    private final long timeoutMs;

    /** @param peers peer id -> host:port (without this node) */
    public GrpcTransport(Map<Integer, String> peers, Duration rpcTimeout) {
        peers.forEach((id, address) -> channels.put(id, Channels.open(address)));
        this.timeoutMs = rpcTimeout.toMillis();
    }

    @Override
    public RequestVoteResponse requestVote(int target, RequestVoteRequest request) throws RpcException {
        try {
            return stub(target).requestVote(request);
        } catch (StatusRuntimeException e) {
            throw new RpcException("RequestVote to " + target + ": " + e.getStatus(), e);
        }
    }

    @Override
    public AppendEntriesResponse appendEntries(int target, AppendEntriesRequest request) throws RpcException {
        try {
            return stub(target).appendEntries(request);
        } catch (StatusRuntimeException e) {
            throw new RpcException("AppendEntries to " + target + ": " + e.getStatus(), e);
        }
    }

    @Override
    public InstallSnapshotResponse installSnapshot(int target, InstallSnapshotRequest request) throws RpcException {
        try {
            return stub(target).installSnapshot(request);
        } catch (StatusRuntimeException e) {
            throw new RpcException("InstallSnapshot to " + target + ": " + e.getStatus(), e);
        }
    }

    private RaftGrpc.RaftBlockingStub stub(int target) throws RpcException {
        ManagedChannel channel = channels.get(target);
        if (channel == null) {
            throw new RpcException("unknown peer " + target);
        }
        return RaftGrpc.newBlockingStub(channel).withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        channels.values().forEach(Channels::close);
    }
}
