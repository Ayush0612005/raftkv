package io.raftkv.harness;

import io.raftkv.raft.MemoryStorage;
import io.raftkv.raft.NodeStatus;
import io.raftkv.raft.NotLeaderException;
import io.raftkv.raft.Proposal;
import io.raftkv.raft.RaftConfig;
import io.raftkv.raft.RaftNode;
import io.raftkv.raft.RaftObserver;
import io.raftkv.raft.RaftStorage;
import io.raftkv.raft.StateMachine;
import io.raftkv.storage.FileStorage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * A cluster of {@code n} nodes (ids 0..n-1) on a {@link SimNetwork}, plus the checks the tests use. Safety is
 * enforced continuously, not only when a test polls:
 * <ul>
 *   <li>Election Safety: two different leaders for one term fail the test;</li>
 *   <li>State Machine Safety: if two nodes apply different commands at the same index, the test fails;</li>
 *   <li>each node must apply indexes in increasing order.</li>
 * </ul>
 * With snapshots on, each node's snapshot also carries the harness's record of what that node applied (like the
 * MIT 3D tester), so a node restored from a snapshot, its own or one installed from the leader, is still checked
 * against everyone else.
 */
public final class SimCluster implements AutoCloseable {

    public static final long ELECTION_TIMEOUT_MS = 1000;

    private final int n;
    private final SimNetwork network = new SimNetwork();
    private final RaftNode[] nodes;
    private final RaftStorage[] storages;
    /** null: in-memory storage; otherwise each node keeps a real WAL in dataDir/node-i. */
    private final Path dataDir;
    /** What each node applied: index -> command. */
    private final List<Map<Long, String>> applied = new ArrayList<>();
    private final long[] lastAppliedIndex;
    private final Map<Long, Integer> leaderOfTerm = new ConcurrentHashMap<>();
    private final List<String> violations = new ArrayList<>();
    /** Creates each node's application state machine (again on every restart). */
    private final Supplier<StateMachine> stateMachineFactory;
    private final StateMachine[] stateMachines;
    /** 0: snapshots off. */
    private final int snapshotThreshold;
    private final AtomicInteger snapshotsInstalled = new AtomicInteger();
    private final AtomicInteger compactions = new AtomicInteger();

    public SimCluster(int n) {
        this(n, null);
    }

    public SimCluster(int n, Path dataDir) {
        this(n, dataDir, EchoStateMachine::new, 0);
    }

    public SimCluster(int n, Path dataDir, Supplier<StateMachine> stateMachineFactory) {
        this(n, dataDir, stateMachineFactory, 0);
    }

    /**
     * @param dataDir             null for in-memory storage, otherwise each node keeps a real {@link FileStorage} there
     * @param stateMachineFactory the application under test ({@link EchoStateMachine} echoes the command back)
     * @param snapshotThreshold   entries between snapshots; 0 turns snapshots off
     */
    public SimCluster(int n, Path dataDir, Supplier<StateMachine> stateMachineFactory, int snapshotThreshold) {
        this.n = n;
        this.dataDir = dataDir;
        this.stateMachineFactory = stateMachineFactory;
        this.snapshotThreshold = snapshotThreshold;
        this.stateMachines = new StateMachine[n];
        this.nodes = new RaftNode[n];
        this.storages = new RaftStorage[n];
        this.lastAppliedIndex = new long[n];
        for (int i = 0; i < n; i++) {
            applied.add(new ConcurrentHashMap<>());
            startNode(i);
        }
    }

    private void startNode(int i) {
        if (dataDir != null) {
            storages[i] = new FileStorage(dataDir.resolve("node-" + i), true);
        } else if (storages[i] == null) {
            storages[i] = new MemoryStorage();
        }
        List<Integer> peers = IntStream.range(0, n).filter(p -> p != i).boxed().toList();
        RaftObserver observer = new RaftObserver() {
            @Override
            public void onBecameLeader(int nodeId, long term) {
                Integer previous = leaderOfTerm.putIfAbsent(term, nodeId);
                if (previous != null && previous != nodeId) {
                    violation("term " + term + " has two leaders: " + previous + " and " + nodeId);
                }
            }

            @Override
            public void onSnapshotInstalled(int nodeId, long lastIncludedIndex) {
                snapshotsInstalled.incrementAndGet();
            }

            @Override
            public void onLogCompacted(int nodeId, long lastIncludedIndex) {
                compactions.incrementAndGet();
            }
        };
        StateMachine application = stateMachineFactory.get();
        stateMachines[i] = application;
        StateMachine recording = new StateMachine() {
            @Override
            public byte[] apply(long index, byte[] command) {
                onApply(i, index, new String(command, StandardCharsets.ISO_8859_1));
                return application.apply(index, command);
            }

            @Override
            public byte[] snapshot() {
                return encodeSnapshot(i, application.snapshot());
            }

            @Override
            public void restore(byte[] snapshot) {
                application.restore(decodeSnapshot(i, snapshot));
            }
        };
        RaftConfig config = RaftConfig.defaults(i, peers).withSnapshotThreshold(snapshotThreshold);
        RaftNode node = new RaftNode(config, network.transportFor(i), storages[i], recording, observer);
        nodes[i] = node;
        network.register(i, node);
        node.start();
    }

    private synchronized void onApply(int server, long index, String command) {
        if (index <= lastAppliedIndex[server]) {
            violation("server " + server + " applied index " + index + " after " + lastAppliedIndex[server]);
        }
        lastAppliedIndex[server] = index;
        for (int j = 0; j < n; j++) {
            String other = applied.get(j).get(index);
            if (other != null && !other.equals(command)) {
                violation("commit index=" + index + " server=" + server + " " + command
                        + " != server=" + j + " " + other);
            }
        }
        applied.get(server).put(index, command);
    }

    /** Harness snapshot = [last applied index, the node's applied commands, the application's own snapshot]. */
    private synchronized byte[] encodeSnapshot(int server, byte[] applicationSnapshot) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeLong(lastAppliedIndex[server]);
            Map<Long, String> log = new TreeMap<>(applied.get(server));
            out.writeInt(log.size());
            for (var e : log.entrySet()) {
                byte[] command = e.getValue().getBytes(StandardCharsets.ISO_8859_1);
                out.writeLong(e.getKey());
                out.writeInt(command.length);
                out.write(command);
            }
            out.writeInt(applicationSnapshot.length);
            out.write(applicationSnapshot);
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Replaces what {@code server} is recorded to have applied with the snapshot's record; returns the app part. */
    private synchronized byte[] decodeSnapshot(int server, byte[] snapshot) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(snapshot));
            long lastIndex = in.readLong();
            int count = in.readInt();
            Map<Long, String> log = new HashMap<>();
            for (int k = 0; k < count; k++) {
                long index = in.readLong();
                byte[] command = in.readNBytes(in.readInt());
                log.put(index, new String(command, StandardCharsets.ISO_8859_1));
            }
            byte[] application = in.readNBytes(in.readInt());
            // what the snapshot says was applied must agree with every other node
            for (var e : log.entrySet()) {
                for (int j = 0; j < n; j++) {
                    String other = applied.get(j).get(e.getKey());
                    if (j != server && other != null && !other.equals(e.getValue())) {
                        violation("snapshot on server " + server + " has " + e.getValue() + " at " + e.getKey()
                                + " but server " + j + " applied " + other);
                    }
                }
            }
            applied.get(server).clear();
            applied.get(server).putAll(log);
            lastAppliedIndex[server] = lastIndex;
            return application;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** InstallSnapshot RPCs that a follower accepted, over the cluster's lifetime. */
    public int snapshotsInstalled() {
        return snapshotsInstalled.get();
    }

    /** Times a node compacted its own log. */
    public int compactions() {
        return compactions.get();
    }

    public RaftStorage storage(int i) {
        return storages[i];
    }

    private void violation(String message) {
        synchronized (violations) {
            violations.add(message);
        }
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

    /** The application state machine of node i (a fresh one after each restart). */
    public StateMachine stateMachine(int i) {
        return stateMachines[i];
    }

    /**
     * Crashes node i: it stops receiving RPCs, its threads stop, and its storage is closed. Writes it had not synced
     * are lost, as with a power failure.
     */
    public void crash(int i) {
        network.unregister(i);
        RaftNode node = nodes[i];
        nodes[i] = null;
        if (node != null) {
            node.close();
        }
    }

    /** Restarts a crashed node from what its storage recovered. It re-applies its log from the start. */
    public void restart(int i) {
        crash(i);
        synchronized (this) {
            applied.get(i).clear();
            lastAppliedIndex[i] = 0;
        }
        startNode(i);
    }

    public boolean isAlive(int i) {
        return nodes[i] != null;
    }

    public void disconnect(int i) {
        network.disconnect(i);
    }

    public void connect(int i) {
        network.connect(i);
    }

    // ------------------------------------------------------------------------------------------- agreement

    /** Proposes on node i; returns the proposal or null if it is not leader. */
    public Proposal start(int i, String command) {
        RaftNode node = nodes[i];
        if (node == null) {
            return null;
        }
        try {
            return node.propose(command.getBytes());
        } catch (NotLeaderException e) {
            return null;
        }
    }

    /** How many nodes applied a command at {@code index}, and which command (null if none). */
    public synchronized Committed nCommitted(int index) {
        return nCommitted((long) index);
    }

    public synchronized Committed nCommitted(long index) {
        checkSafety();
        int count = 0;
        String command = null;
        for (int i = 0; i < n; i++) {
            String c = applied.get(i).get(index);
            if (c != null) {
                count++;
                command = c;
            }
        }
        return new Committed(count, command);
    }

    public record Committed(int count, String command) {
    }

    /**
     * Gets {@code command} committed on at least {@code expectedServers} nodes, MIT-style: find a leader, propose,
     * wait up to 2 s for agreement; if {@code retry}, try again with a (possibly new) leader for up to 10 s.
     * Returns the log index.
     */
    public long one(String command, int expectedServers, boolean retry) {
        long t0 = System.nanoTime();
        int starts = 0;
        while (System.nanoTime() - t0 < 10_000_000_000L) {
            long index = -1;
            for (int si = 0; si < n; si++) {
                starts = (starts + 1) % n;
                if (network.isConnected(starts) && isAlive(starts)) {
                    Proposal p = start(starts, command);
                    if (p != null) {
                        index = p.index();
                        break;
                    }
                }
            }
            if (index != -1) {
                long t1 = System.nanoTime();
                while (System.nanoTime() - t1 < 2_000_000_000L) {
                    Committed c = nCommitted(index);
                    if (c.count() >= expectedServers && command.equals(c.command())) {
                        return index;
                    }
                    sleep(20);
                }
                if (!retry) {
                    fail("one(" + command + ") failed to reach agreement");
                }
            } else {
                sleep(50);
            }
        }
        fail("one(" + command + ") failed to reach agreement");
        return -1;
    }

    /** Waits until at least {@code count} nodes applied {@code index}; returns the command. */
    public String waitCommitted(long index, int count) {
        for (int attempt = 0; attempt < 300; attempt++) {
            Committed c = nCommitted(index);
            if (c.count() >= count) {
                return c.command();
            }
            sleep(10);
        }
        fail("only " + nCommitted(index).count() + " of " + count + " servers committed index " + index);
        return null;
    }

    /**
     * The Phase 2 gate: once the cluster is quiet, every node has applied exactly the same commands at the same
     * indexes. Waits up to 10 s for all nodes to catch up to the same applied index.
     */
    public void checkLogsIdentical() {
        for (int attempt = 0; attempt < 200; attempt++) {
            long[] appliedIdx = new long[n];
            for (int i = 0; i < n; i++) {
                appliedIdx[i] = nodes[i].status().lastApplied();
            }
            long first = appliedIdx[0];
            if (Arrays.stream(appliedIdx).allMatch(x -> x == first) && first > 0) {
                synchronized (this) {
                    Map<Long, String> reference = new HashMap<>(applied.get(0));
                    for (int i = 1; i < n; i++) {
                        if (!reference.equals(applied.get(i))) {
                            fail("server 0 and server " + i + " applied different logs");
                        }
                    }
                }
                checkSafety();
                return;
            }
            sleep(50);
        }
        fail("servers did not converge to the same applied index");
    }

    // ----------------------------------------------------------------------------------------------- checks

    /** Fails if any safety violation was recorded so far. */
    public void checkSafety() {
        synchronized (violations) {
            if (!violations.isEmpty()) {
                fail("safety violated: " + violations);
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
                RaftNode node = nodes[i];
                if (node != null && network.isConnected(i)) {
                    NodeStatus status = node.status();
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
            RaftNode node = nodes[i];
            if (node != null && network.isConnected(i)) {
                long t = node.status().term();
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
            RaftNode node = nodes[i];
            if (node != null && network.isConnected(i) && node.status().isLeader()) {
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
