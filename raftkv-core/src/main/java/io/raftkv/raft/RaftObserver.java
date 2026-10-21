package io.raftkv.raft;

/** Hooks for tests and metrics. Called with the node's lock held: implementations must be fast and must not block. */
public interface RaftObserver {

    RaftObserver NONE = new RaftObserver() {};

    default void onBecameLeader(int nodeId, long term) {}

    /** A follower replaced its log with a snapshot from the leader. */
    default void onSnapshotInstalled(int nodeId, long lastIncludedIndex) {}

    /** The node compacted its own log up to {@code lastIncludedIndex}. */
    default void onLogCompacted(int nodeId, long lastIncludedIndex) {}
}
