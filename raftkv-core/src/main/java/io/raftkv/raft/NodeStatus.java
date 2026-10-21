package io.raftkv.raft;

/**
 * A consistent snapshot of a node's externally interesting state. {@code logEntries} is the number of entries held
 * after {@code snapshotIndex}; with snapshots on it stays bounded.
 */
public record NodeStatus(int id, Role role, long term, int leaderId, long lastLogIndex, long commitIndex,
                         long lastApplied, long snapshotIndex, int logEntries) {

    public boolean isLeader() {
        return role == Role.LEADER;
    }
}
