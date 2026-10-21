package io.raftkv.raft;

import io.raftkv.client.RaftKvClient;
import io.raftkv.harness.EchoStateMachine;
import io.raftkv.harness.SimCluster;
import io.raftkv.harness.SimKvEndpoint;
import io.raftkv.kv.KvStateMachine;
import io.raftkv.storage.FileStorage;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.raftkv.harness.SimCluster.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Phase 5 gate: log size bounded. Modelled on MIT 6.5840 lab 3D, plus a disk-level bound on the real WAL. */
@Tag("fault")
class SnapshotTest {

    private static final int THRESHOLD = 20;

    @TempDir
    Path dataDir;

    private static SimCluster cluster(int n) {
        return new SimCluster(n, null, EchoStateMachine::new, THRESHOLD);
    }

    private static String rand() {
        return String.valueOf(ThreadLocalRandom.current().nextLong());
    }

    /** Every connected node holds few entries in memory and in storage, however long the history. */
    private static void checkLogBounded(SimCluster cluster) {
        for (int i = 0; i < cluster.size(); i++) {
            if (!cluster.isAlive(i)) {
                continue;
            }
            NodeStatus s = cluster.node(i).status();
            // threshold entries before a snapshot, plus whatever arrived while it was being taken
            assertTrue(s.logEntries() <= 3 * THRESHOLD,
                    "node " + i + " holds " + s.logEntries() + " entries (snapshot at " + s.snapshotIndex() + ")");
            if (cluster.storage(i) instanceof MemoryStorage m) {
                assertTrue(m.storedEntries() <= 3 * THRESHOLD, "node " + i + " stores " + m.storedEntries());
            }
        }
    }

    @Test
    void snapshotsKeepTheLogBounded() {
        try (SimCluster cluster = cluster(3)) {
            for (int i = 0; i < 10 * THRESHOLD; i++) {
                cluster.one(rand(), 3, true);
            }
            cluster.checkLogsIdentical();
            checkLogBounded(cluster);
            assertTrue(cluster.compactions() >= 3 * 5, "every node should have compacted several times");
            for (int i = 0; i < 3; i++) {
                assertTrue(cluster.node(i).status().snapshotIndex() >= 8 * THRESHOLD);
            }
        }
    }

    /** A follower that missed more than the leader still holds must be brought up to date with InstallSnapshot. */
    @Test
    void laggingFollowerCatchesUpViaInstallSnapshot() {
        try (SimCluster cluster = cluster(3)) {
            cluster.one(rand(), 3, true);
            int follower = (cluster.checkOneLeader() + 1) % 3;
            cluster.disconnect(follower);
            for (int i = 0; i < 5 * THRESHOLD; i++) {
                cluster.one(rand(), 2, true);
            }
            assertEquals(0, cluster.snapshotsInstalled());
            cluster.connect(follower);
            cluster.one(rand(), 3, true);
            assertTrue(cluster.snapshotsInstalled() >= 1, "the follower should have received a snapshot");
            cluster.checkLogsIdentical();
            checkLogBounded(cluster);
        }
    }

    @Test
    void crashedFollowerRecoversViaInstallSnapshot() {
        try (SimCluster cluster = cluster(3)) {
            cluster.one(rand(), 3, true);
            int follower = (cluster.checkOneLeader() + 1) % 3;
            cluster.crash(follower);
            for (int i = 0; i < 5 * THRESHOLD; i++) {
                cluster.one(rand(), 2, true);
            }
            cluster.restart(follower);
            cluster.one(rand(), 3, true);
            assertTrue(cluster.snapshotsInstalled() >= 1);
            cluster.checkLogsIdentical();
        }
    }

    /** Everyone restarts from their own snapshot plus the log after it. */
    @Test
    void wholeClusterRestartsFromSnapshots() {
        try (SimCluster cluster = cluster(3)) {
            for (int i = 0; i < 4 * THRESHOLD + 7; i++) {
                cluster.one(rand(), 3, true);
            }
            for (int i = 0; i < 3; i++) {
                cluster.crash(i);
            }
            for (int i = 0; i < 3; i++) {
                cluster.restart(i);
                assertTrue(cluster.node(i).status().lastApplied() >= 3 * THRESHOLD,
                        "node " + i + " should start from its snapshot, not from index 0");
            }
            cluster.one(rand(), 3, true);
            cluster.checkLogsIdentical();
        }
    }

    /** MIT 3D "install snapshots (unreliable+crash)": lossy network, crashes, partitions, snapshots everywhere. */
    @Test
    void snapshotsUnderCrashesAndUnreliableNetwork() {
        int n = 3;
        try (SimCluster cluster = cluster(n)) {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            cluster.network().setReliable(false);
            cluster.one(rand(), 1, true);
            for (int iteration = 0; iteration < 12; iteration++) {
                int victim = random.nextInt(n);
                boolean crash = random.nextBoolean();
                if (crash) {
                    cluster.crash(victim);
                } else {
                    cluster.disconnect(victim);
                }
                for (int i = 0; i < THRESHOLD + 1; i++) {
                    cluster.one(rand(), n - 1, true);
                }
                if (crash) {
                    cluster.restart(victim);
                } else {
                    cluster.connect(victim);
                }
                cluster.one(rand(), n, true);
            }
            cluster.network().setReliable(true);
            cluster.one(rand(), n, true);
            cluster.checkLogsIdentical();
            checkLogBounded(cluster);
        }
    }

    /**
     * The gate on disk: KV clients write ~2,000 values of 100 bytes through the real client library onto real WALs.
     * Without compaction each WAL would grow past 300 KB; with a snapshot every 100 entries it must stay small the
     * whole time. Afterwards the whole cluster restarts from disk with its data intact.
     */
    @Test
    void walSizeStaysBoundedOnDisk() throws Exception {
        int threshold = 100;
        String padding = "x".repeat(100);
        AtomicLong maxWalBytes = new AtomicLong();
        AtomicInteger maxEntries = new AtomicInteger();
        try (SimCluster cluster = new SimCluster(3, dataDir, KvStateMachine::new, threshold)) {
            AtomicBoolean done = new AtomicBoolean();
            Thread monitor = Thread.ofVirtual().start(() -> {
                while (!done.get()) {
                    for (int i = 0; i < 3; i++) {
                        maxWalBytes.accumulateAndGet(((FileStorage) cluster.storage(i)).sizeBytes(), Math::max);
                        maxEntries.accumulateAndGet(cluster.node(i).status().logEntries(), Math::max);
                    }
                    sleep(5);
                }
            });
            List<Thread> clients = new ArrayList<>();
            for (int c = 0; c < 5; c++) {
                int clientNo = c;
                clients.add(Thread.ofVirtual().start(() -> {
                    try (RaftKvClient client = new RaftKvClient(new SimKvEndpoint(cluster), Duration.ofSeconds(30))) {
                        for (int k = 0; k < 400; k++) {
                            client.put("c" + clientNo + "-k" + (k % 50), k + "-" + padding);
                        }
                    }
                }));
            }
            for (Thread t : clients) {
                t.join();
            }
            done.set(true);
            monitor.join();

            long applied = cluster.node(cluster.checkOneLeader()).status().lastApplied();
            System.out.printf("walSizeStaysBoundedOnDisk: %d entries applied, max WAL %d bytes, max in-memory log %d"
                    + " entries, %d compactions%n", applied, maxWalBytes.get(), maxEntries.get(), cluster.compactions());
            assertTrue(applied >= 2000);
            assertTrue(maxWalBytes.get() < 64 * 1024, "WAL grew to " + maxWalBytes.get() + " bytes");
            assertTrue(maxEntries.get() <= 3 * threshold, "in-memory log reached " + maxEntries.get() + " entries");

            // full restart from snapshot + WAL
            for (int i = 0; i < 3; i++) {
                cluster.crash(i);
            }
            for (int i = 0; i < 3; i++) {
                cluster.restart(i);
            }
            try (RaftKvClient client = new RaftKvClient(new SimKvEndpoint(cluster), Duration.ofSeconds(30))) {
                for (int c = 0; c < 5; c++) {
                    assertEquals(Optional.of("399-" + padding), client.get("c" + c + "-k49"));
                }
            }
            cluster.checkLogsIdentical();
        }
    }

    @Test
    void snapshotsOffMeansNoCompaction() {
        try (SimCluster cluster = new SimCluster(3)) {
            for (int i = 0; i < 30; i++) {
                cluster.one(rand(), 3, true);
            }
            assertEquals(0, cluster.compactions());
            assertEquals(0, cluster.node(0).status().snapshotIndex());
        }
    }
}
