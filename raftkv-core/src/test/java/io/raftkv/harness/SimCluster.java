package io.raftkv.harness;

import io.raftkv.raft.MemoryStorage;
import io.raftkv.raft.NodeStatus;
import io.raftkv.raft.RaftConfig;
import io.raftkv.raft.RaftNode;
import io.raftkv.raft.RaftObserver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * A cluster of {@code n} nodes (ids 0..n-1) on a {@link SimNetwork}, plus the checks the tests use. It also enforces
 * Election Safety (§5.2, Figure 3) continuously: every node reports when it becomes leader, and two different
 * leaders for the same term fail the test even if no polling check happened to see both.
 */
public final class SimCluster implements AutoCloseable {

    public static final long ELECTION_TIMEOUT_MS = 1000;

    private final int n;
    private final SimNetwork network = new SimNetwork();
    private final RaftNode[] nodes;
    private final MemoryStorage[] storages;
    private final Map<Long, Integer> leaderOfTerm = new ConcurrentHashMap<>();
    private final List<String> violations = new ArrayList<>();

    public SimCluster(int n) {
        this.n = n;
        this.nodes = new RaftNode[n];
        this.storages = new MemoryStorage[n];
        for (int i = 0; i < n; i++) {
            storages[i] = new MemoryStorage();
            startNode(i);
        }
    }

    private void startNode(int i) {
        List<Integer> peers = IntStream.range(0, n).filter(p -> p != i).boxed().toList();
        RaftObserver observer = new RaftObserver() {
            @Override
            public void onBecameLeader(int nodeId, long term) {
                Integer previous = leaderOfTerm.putIfAbsent(term, nodeId);
                if (previous != null && previous != nodeId) {
                    synchronized (violations) {
                        violations.add("term " + term + " has two leaders: " + previous + " and " + nodeId);
                    }
                }
            }
        };
        RaftNode node = new RaftNode(RaftConfig.defaults(i, peers), network.transportFor(i), storages[i], observer);
        nodes[i] = node;
        network.register(i, node);
        node.start();
    }

    public int size() {
        return n;
    }

    public SimNetwork network() {
        return network;
    }

    public RaftNode node(int i) {
        return nodes[i];
    }

    public void disconnect(int i) {
        network.disconnect(i);
    }

    public void connect(int i) {
        network.connect(i);
    }

    // ----------------------------------------------------------------------------------------------- checks

    /** Fails if any safety violation was recorded so far. */
    public void checkSafety() {
        synchronized (violations) {
            if (!violations.isEmpty()) {
                fail("election safety violated: " + violations);
            }
        }
    }

    /**
     * Waits until exactly one connected node believes it is leader in the newest term and returns it. Fails if a
     * term ever has two leaders or no leader shows up within ~5 s.
     */
    public int checkOneLeader() {
        for (int iteration = 0; iteration < 10; iteration++) {
            sleep(450 + ThreadLocalRandom.current().nextInt(100));
            Map<Long, List<Integer>> leaders = new HashMap<>();
            for (int i = 0; i < n; i++) {
                if (network.isConnected(i)) {
                    NodeStatus status = nodes[i].status();
                    if (status.isLeader()) {
                        leaders.computeIfAbsent(status.term(), t -> new ArrayList<>()).add(i);
                    }
                }
            }
            long lastTerm = -1;
            for (var entry : leaders.entrySet()) {
                if (entry.getValue().size() > 1) {
                    fail("term " + entry.getKey() + " has " + entry.getValue().size() + " leaders: " + entry.getValue());
                }
                lastTerm = Math.max(lastTerm, entry.getKey());
            }
            if (!leaders.isEmpty()) {
                checkSafety();
                return leaders.get(lastTerm).getFirst();
            }
        }
        fail("expected one leader, got none");
        return -1;
    }

    /** All connected nodes agree on the current term; returns it. */
    public long checkTerms() {
        long term = -1;
        for (int i = 0; i < n; i++) {
            if (network.isConnected(i)) {
                long t = nodes[i].status().term();
                if (term == -1) {
                    term = t;
                } else if (term != t) {
                    fail("servers disagree on term: " + term + " vs " + t);
                }
            }
        }
        return term;
    }

    /** No connected node believes it is leader. */
    public void checkNoLeader() {
        for (int i = 0; i < n; i++) {
            if (network.isConnected(i) && nodes[i].status().isLeader()) {
                fail("expected no leader among connected servers, but " + i + " claims to be leader");
            }
        }
    }

    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        for (RaftNode node : nodes) {
            if (node != null) {
                node.close();
            }
        }
        checkSafety();
    }
}
