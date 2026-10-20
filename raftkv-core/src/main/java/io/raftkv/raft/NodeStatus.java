package io.raftkv.raft;

/** A consistent snapshot of a node's externally interesting state. */
public record NodeStatus(int id, Role role, long term, int leaderId) {

    public boolean isLeader() {
        return role == Role.LEADER;
    }
}
