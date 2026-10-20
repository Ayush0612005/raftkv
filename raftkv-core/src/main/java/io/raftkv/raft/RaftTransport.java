package io.raftkv.raft;

import io.raftkv.proto.AppendEntriesRequest;
import io.raftkv.proto.AppendEntriesResponse;
import io.raftkv.proto.RequestVoteRequest;
import io.raftkv.proto.RequestVoteResponse;

/**
 * Outgoing side of node-to-node communication. Calls block until a response arrives or the RPC fails; nodes call
 * them from virtual threads, so blocking is cheap. Implementations: gRPC (real deployments) and the simulated
 * network used by the fault-injection tests.
 */
public interface RaftTransport {

    RequestVoteResponse requestVote(int target, RequestVoteRequest request) throws RpcException;

    AppendEntriesResponse appendEntries(int target, AppendEntriesRequest request) throws RpcException;
}
