package io.raftkv.raft;

/** The three states a Raft server can be in (§5.1). */
public enum Role {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
