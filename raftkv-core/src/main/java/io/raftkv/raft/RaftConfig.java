package io.raftkv.raft;

import java.util.List;

/**
 * Static configuration of one node. Node ids are small integers; {@code peers} are the other members.
 *
 * <p>Timing follows §5.6: broadcastTime &lt;&lt; electionTimeout. Heartbeats go out every {@code heartbeatIntervalMs};
 * a follower that hears nothing for a random time in [min, max) starts an election.
 */
public record RaftConfig(int id, List<Integer> peers, long electionTimeoutMinMs, long electionTimeoutMaxMs,
                         long heartbeatIntervalMs) {

    public RaftConfig {
        peers = List.copyOf(peers);
        if (peers.contains(id)) {
            throw new IllegalArgumentException("peers must not contain the node itself: " + id);
        }
        if (peers.stream().distinct().count() != peers.size()) {
            throw new IllegalArgumentException("duplicate peer ids: " + peers);
        }
        if (electionTimeoutMinMs <= 0 || electionTimeoutMaxMs <= electionTimeoutMinMs) {
            throw new IllegalArgumentException("need 0 < electionTimeoutMin < electionTimeoutMax");
        }
        if (heartbeatIntervalMs <= 0 || heartbeatIntervalMs * 2 > electionTimeoutMinMs) {
            throw new IllegalArgumentException("heartbeat interval must be well below the election timeout");
        }
    }

    public static RaftConfig defaults(int id, List<Integer> peers) {
        return new RaftConfig(id, peers, 150, 300, 40);
    }

    public int clusterSize() {
        return peers.size() + 1;
    }

    /** Votes or replicas needed for a decision, counting this node. */
    public int majority() {
        return clusterSize() / 2 + 1;
    }
}
