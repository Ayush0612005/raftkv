package io.raftkv.raft;

/**
 * This node cannot accept or finish a proposal because it is not (or no longer) the leader. The client should
 * retry, preferably at {@link #leaderHint()} if known ({@link RaftNode#NONE} otherwise).
 */
public final class NotLeaderException extends Exception {

    private final int leaderHint;

    public NotLeaderException(int leaderHint) {
        super(leaderHint == RaftNode.NONE ? "not leader, leader unknown" : "not leader, try node " + leaderHint,
                null, false, false);
        this.leaderHint = leaderHint;
    }

    public int leaderHint() {
        return leaderHint;
    }
}
