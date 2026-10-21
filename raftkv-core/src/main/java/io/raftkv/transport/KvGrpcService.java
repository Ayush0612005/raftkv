package io.raftkv.transport;

import io.grpc.stub.StreamObserver;
import io.raftkv.kv.KvService;
import io.raftkv.proto.KvCommand;
import io.raftkv.proto.KvGrpc;
import io.raftkv.proto.KvResponse;
import io.raftkv.proto.StatusRequest;
import io.raftkv.proto.StatusResponse;
import io.raftkv.raft.NodeStatus;
import io.raftkv.raft.RaftNode;

/** Server side of the client API. Calls block until committed; the server runs them on virtual threads. */
public final class KvGrpcService extends KvGrpc.KvImplBase {

    private final KvService kv;
    private final RaftNode node;

    public KvGrpcService(KvService kv, RaftNode node) {
        this.kv = kv;
        this.node = node;
    }

    @Override
    public void execute(KvCommand command, StreamObserver<KvResponse> observer) {
        observer.onNext(kv.execute(command));
        observer.onCompleted();
    }

    @Override
    public void status(StatusRequest request, StreamObserver<StatusResponse> observer) {
        NodeStatus s = node.status();
        observer.onNext(StatusResponse.newBuilder()
                .setId(s.id())
                .setRole(s.role().name())
                .setTerm(s.term())
                .setLeaderId(s.leaderId())
                .setLastLogIndex(s.lastLogIndex())
                .setCommitIndex(s.commitIndex())
                .setLastApplied(s.lastApplied())
                .setSnapshotIndex(s.snapshotIndex())
                .setLogEntries(s.logEntries())
                .build());
        observer.onCompleted();
    }
}
