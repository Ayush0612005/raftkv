package io.raftkv.transport;

import io.grpc.stub.StreamObserver;
import io.raftkv.proto.AppendEntriesRequest;
import io.raftkv.proto.AppendEntriesResponse;
import io.raftkv.proto.InstallSnapshotRequest;
import io.raftkv.proto.InstallSnapshotResponse;
import io.raftkv.proto.RaftGrpc;
import io.raftkv.proto.RequestVoteRequest;
import io.raftkv.proto.RequestVoteResponse;
import io.raftkv.raft.RaftRpcHandler;

/** Server side of node-to-node RPCs: hands each call to the node. */
public final class RaftGrpcService extends RaftGrpc.RaftImplBase {

    private final RaftRpcHandler handler;

    public RaftGrpcService(RaftRpcHandler handler) {
        this.handler = handler;
    }

    @Override
    public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> observer) {
        observer.onNext(handler.handleRequestVote(request));
        observer.onCompleted();
    }

    @Override
    public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> observer) {
        observer.onNext(handler.handleAppendEntries(request));
        observer.onCompleted();
    }

    @Override
    public void installSnapshot(InstallSnapshotRequest request, StreamObserver<InstallSnapshotResponse> observer) {
        observer.onNext(handler.handleInstallSnapshot(request));
        observer.onCompleted();
    }
}
