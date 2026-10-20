package io.raftkv.raft;

import io.raftkv.harness.SimCluster;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.raftkv.harness.SimCluster.ELECTION_TIMEOUT_MS;
import static io.raftkv.harness.SimCluster.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Phase 3 gate: a restarted node catches up. Modelled on MIT 6.5840 lab 3C, plus tests on the real WAL. */
class PersistenceTest {

    @TempDir
    Path dataDir;

    private static String rand() {
        return String.valueOf(ThreadLocalRandom.current().nextLong());
    }

    @Test
    void basicPersistence() {
        try (SimCluster cluster = new SimCluster(3)) {
            cluster.one("11", 3, true);

            // crash and restart everyone
            for (int i = 0; i < 3; i++) {
                cluster.restart(i);
            }
            cluster.one("12", 3, true);

            int leader1 = cluster.checkOneLeader();
            cluster.restart(leader1);
            cluster.one("13", 3, true);

            int leader2 = cluster.checkOneLeader();
            cluster.crash(leader2);
            cluster.one("14", 2, true);
            cluster.restart(leader2);
            long index = cluster.one("14b", 3, true);
            cluster.waitCommitted(index, 3);

            int i3 = (cluster.checkOneLeader() + 1) % 3;
            cluster.crash(i3);
            cluster.one("15", 2, true);
            cluster.restart(i3);
            cluster.one("16", 3, true);
            cluster.checkLogsIdentical();
        }
    }

    @Test
    void moreCrashes() {
        try (SimCluster cluster = new SimCluster(5)) {
            int index = 1;
            for (int iteration = 0; iteration < 5; iteration++) {
                cluster.one(String.valueOf(10 + index++), 5, true);
                int leader1 = cluster.checkOneLeader();

                cluster.crash((leader1 + 1) % 5);
                cluster.crash((leader1 + 2) % 5);
                cluster.one(String.valueOf(10 + index++), 3, true);

                cluster.crash(leader1);
                cluster.crash((leader1 + 3) % 5);
                cluster.crash((leader1 + 4) % 5);

                cluster.restart((leader1 + 1) % 5);
                cluster.restart((leader1 + 2) % 5);
                sleep(ELECTION_TIMEOUT_MS);
                cluster.restart((leader1 + 3) % 5);

                cluster.one(String.valueOf(10 + index++), 3, true);

                cluster.restart((leader1 + 4) % 5);
                cluster.restart(leader1);
            }
            cluster.one("1000", 5, true);
            cluster.checkLogsIdentical();
        }
    }

    @Test
    void partitionedLeaderAndCrashedFollower() {
        try (SimCluster cluster = new SimCluster(3)) {
            cluster.one("101", 3, true);
            int leader = cluster.checkOneLeader();
            cluster.disconnect((leader + 2) % 3);

            cluster.one("102", 2, true);

            cluster.crash(leader % 3);
            cluster.crash((leader + 1) % 3);
            cluster.connect((leader + 2) % 3);
            cluster.restart(leader % 3);

            cluster.one("103", 2, true);

            cluster.restart((leader + 1) % 3);
            cluster.one("104", 3, true);
            cluster.checkLogsIdentical();
        }
    }

    /**
     * Figure 8 of the paper: leaders crash right after proposing, so entries from old terms float around in
     * partially replicated logs. A leader must never commit an old-term entry by counting replicas alone.
     */
    @Test
    void figure8() {
        int n = 5;
        try (SimCluster cluster = new SimCluster(n)) {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            cluster.one(rand(), 1, true);
            int up = n;
            for (int iteration = 0; iteration < 300; iteration++) {
                int leader = -1;
                for (int i = 0; i < n; i++) {
                    if (cluster.isAlive(i) && cluster.start(i, rand()) != null) {
                        leader = i;
                    }
                }
                if (random.nextInt(1000) < 100) {
                    sleep(random.nextLong(ELECTION_TIMEOUT_MS / 2));
                } else {
                    sleep(random.nextLong(13));
                }
                if (leader != -1) {
                    cluster.crash(leader);
                    up--;
                }
                if (up < 3) {
                    int s = random.nextInt(n);
                    if (!cluster.isAlive(s)) {
                        cluster.restart(s);
                        up++;
                    }
                }
            }
            for (int i = 0; i < n; i++) {
                if (!cluster.isAlive(i)) {
                    cluster.restart(i);
                }
            }
            cluster.one(rand(), n, true);
            cluster.checkLogsIdentical();
        }
    }

    /** Figure 8 again, over a network that drops, delays and reorders messages, with partitions instead of crashes. */
    @Test
    void figure8Unreliable() {
        int n = 5;
        try (SimCluster cluster = new SimCluster(n)) {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            cluster.network().setReliable(false);
            cluster.one(String.valueOf(random.nextInt(10000)), 1, true);
            int up = n;
            for (int iteration = 0; iteration < 300; iteration++) {
                if (iteration == 100) {
                    cluster.network().setLongReordering(true);
                }
                int leader = -1;
                for (int i = 0; i < n; i++) {
                    if (cluster.network().isConnected(i)
                            && cluster.start(i, String.valueOf(random.nextInt(10000))) != null) {
                        leader = i;
                    }
                }
                if (random.nextInt(1000) < 100) {
                    sleep(random.nextLong(ELECTION_TIMEOUT_MS / 2));
                } else {
                    sleep(random.nextLong(13));
                }
                if (leader != -1 && random.nextInt(1000) < ELECTION_TIMEOUT_MS / 2) {
                    cluster.disconnect(leader);
                    up--;
                }
                if (up < 3) {
                    int s = random.nextInt(n);
                    if (!cluster.network().isConnected(s)) {
                        cluster.connect(s);
                        up++;
                    }
                }
            }
            for (int i = 0; i < n; i++) {
                cluster.connect(i);
            }
            cluster.network().setLongReordering(false);
            cluster.network().setReliable(true);
            cluster.one(String.valueOf(random.nextInt(10000)), n, true);
            cluster.checkLogsIdentical();
        }
    }

    /** Clients keep proposing while nodes are randomly crashed, restarted, disconnected and reconnected. */
    @Test
    void unreliableChurn() throws Exception {
        int n = 5;
        try (SimCluster cluster = new SimCluster(n)) {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            cluster.network().setReliable(false);
            AtomicBoolean stop = new AtomicBoolean();
            List<Thread> clients = new ArrayList<>();
            for (int c = 0; c < 3; c++) {
                int client = c;
                clients.add(Thread.ofVirtual().start(() -> {
                    int k = 0;
                    while (!stop.get()) {
                        for (int i = 0; i < n; i++) {
                            cluster.start(i, client + "-" + k++);
                        }
                        sleep(ThreadLocalRandom.current().nextInt(79) + 17);
                    }
                }));
            }
            long end = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < end) {
                int i = random.nextInt(n);
                switch (random.nextInt(4)) {
                    case 0 -> cluster.disconnect(i);
                    case 1 -> cluster.connect(i);
                    case 2 -> {
                        if (cluster.isAlive(i)) {
                            cluster.crash(i);
                        }
                    }
                    default -> {
                        if (!cluster.isAlive(i)) {
                            cluster.restart(i);
                        }
                    }
                }
                // keep a majority alive most of the time
                int alive = 0;
                for (int j = 0; j < n; j++) {
                    alive += cluster.isAlive(j) ? 1 : 0;
                }
                if (alive <= n / 2) {
                    for (int j = 0; j < n; j++) {
                        if (!cluster.isAlive(j)) {
                            cluster.restart(j);
                            break;
                        }
                    }
                }
                sleep(random.nextLong(ELECTION_TIMEOUT_MS * 7 / 10));
            }
            sleep(ELECTION_TIMEOUT_MS);
            cluster.network().setReliable(true);
            for (int i = 0; i < n; i++) {
                if (!cluster.isAlive(i)) {
                    cluster.restart(i);
                }
                cluster.connect(i);
            }
            stop.set(true);
            for (Thread t : clients) {
                t.join();
            }
            cluster.one(rand(), n, true);
            cluster.checkLogsIdentical();
        }
    }

    // ------------------------------------------------------------------ the same guarantees on the real WAL

    @Test
    void restartedNodeCatchesUpFromRealWal() {
        try (SimCluster cluster = new SimCluster(3, dataDir)) {
            for (int i = 0; i < 20; i++) {
                cluster.one("a" + i, 3, true);
            }
            int follower = (cluster.checkOneLeader() + 1) % 3;
            cluster.crash(follower);
            for (int i = 0; i < 100; i++) {
                cluster.one("b" + i, 2, true);
            }
            cluster.restart(follower);
            long last = cluster.one("c", 3, true);
            assertEquals("c", cluster.waitCommitted(last, 3));
            cluster.checkLogsIdentical();
        }
    }

    @Test
    void wholeClusterRestartKeepsCommittedEntries() {
        long lastBefore;
        try (SimCluster cluster = new SimCluster(3, dataDir)) {
            for (int i = 0; i < 30; i++) {
                cluster.one("x" + i, 3, true);
            }
            lastBefore = cluster.one("before-restart", 3, true);
            for (int i = 0; i < 3; i++) {
                cluster.crash(i);
            }
            for (int i = 0; i < 3; i++) {
                cluster.restart(i);
            }
            long after = cluster.one("after-restart", 3, true);
            assertTrue(after > lastBefore, "a new entry must go after the recovered ones");
            // the entry committed before the restart is still there, at the same index, on every node
            assertEquals("before-restart", cluster.waitCommitted(lastBefore, 3));
            cluster.checkLogsIdentical();
        }
    }
}
