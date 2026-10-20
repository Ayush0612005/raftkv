package io.raftkv.raft;

/** Hooks for tests and metrics. Called with the node's lock held: implementations must be fast and must not block. */
public interface RaftObserver {

    RaftObserver NONE = new RaftObserver() {};

    default void onBecameLeader(int nodeId, long term) {}
}
