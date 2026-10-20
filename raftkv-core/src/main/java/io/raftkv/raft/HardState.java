package io.raftkv.raft;

/** The persistent state that must survive a crash before a node answers any RPC: current term and vote. */
public record HardState(long term, int votedFor) {

    public static final HardState INITIAL = new HardState(0, RaftNode.NONE);
}
