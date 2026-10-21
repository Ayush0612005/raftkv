package io.raftkv.raft;

import io.raftkv.proto.AppendEntriesRequest;
import io.raftkv.proto.AppendEntriesResponse;
import io.raftkv.proto.InstallSnapshotRequest;
import io.raftkv.proto.InstallSnapshotResponse;
import io.raftkv.proto.RequestVoteRequest;
import io.raftkv.proto.RequestVoteResponse;

/** Incoming side of node-to-node communication: what a transport server dispatches to. */
public interface RaftRpcHandler {

    RequestVoteResponse handleRequestVote(RequestVoteRequest request);

    AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request);

    InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request);
}
