package io.raftkv.raft;

import io.raftkv.harness.SimCluster;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static io.raftkv.harness.SimCluster.ELECTION_TIMEOUT_MS;
import static io.raftkv.harness.SimCluster.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Phase 2 gate: logs identical on all nodes. Modelled on MIT 6.5840 lab 3B. */
@Tag("fault")
class ReplicationTest {

    @Test
    void basicAgreement() {
        try (SimCluster cluster = new SimCluster(3)) {
            for (int i = 1; i <= 3; i++) {
                cluster.one("cmd-" + i, 3, false);
            }
            cluster.checkLogsIdentical();
        }
    }

    @Test
    void agreementDespiteFollowerDisconnection() {
        try (SimCluster cluster = new SimCluster(3)) {
            cluster.one("101", 3, false);

            int leader = cluster.checkOneLeader();
            cluster.disconnect((leader + 1) % 3);

            // the leader and the remaining follower still make progress
            cluster.one("102", 2, false);
            cluster.one("103", 2, false);
            sleep(ELECTION_TIMEOUT_MS);
            cluster.one("104", 2, false);

            // follower comes back and catches up
            cluster.connect((leader + 1) % 3);
            cluster.one("105", 3, true);
            cluster.checkLogsIdentical();
        }
    }

    @Test
    void noAgreementWithoutQuorum() {
        try (SimCluster cluster = new SimCluster(5)) {
            cluster.one("10", 5, false);

            int leader = cluster.checkOneLeader();
            cluster.disconnect((leader + 1) % 5);
            cluster.disconnect((leader + 2) % 5);
            cluster.disconnect((leader + 3) % 5);

            Proposal p = cluster.start(leader, "20");
            assertNotNull(p, "leader rejected the proposal");
            sleep(2 * ELECTION_TIMEOUT_MS);
            assertEquals(0, cluster.nCommitted(p.index()).count(), "committed without a majority");

            // repair: the cluster commits again (the uncommitted entry may be kept or replaced)
            cluster.connect((leader + 1) % 5);
            cluster.connect((leader + 2) % 5);
            cluster.connect((leader + 3) % 5);
            cluster.one("1000", 5, true);
            cluster.checkLogsIdentical();
        }
    }

    @Test
    void concurrentProposals() throws Exception {
        try (SimCluster cluster = new SimCluster(3)) {
            for (int attempt = 0; attempt < 5; attempt++) {
                int leader = cluster.checkOneLeader();
                Proposal first = cluster.start(leader, "first");
                if (first == null) {
                    continue; // leader moved on; try again
                }
                int iters = 5;
                List<Thread> threads = new ArrayList<>();
                List<Proposal> proposals = java.util.Collections.synchronizedList(new ArrayList<>());
                for (int i = 0; i < iters; i++) {
                    int x = 100 + i;
                    threads.add(Thread.ofVirtual().start(() -> {
                        Proposal p = cluster.start(leader, String.valueOf(x));
                        if (p != null) {
                            proposals.add(p);
                        }
                    }));
                }
                for (Thread t : threads) {
                    t.join();
                }
                if (proposals.size() != iters || proposals.stream().anyMatch(p -> p.term() != first.term())) {
                    continue; // term changed while proposing; try again
                }
                Set<String> committed = new HashSet<>();
                for (Proposal p : proposals) {
                    committed.add(cluster.waitCommitted(p.index(), 3));
                }
                for (int i = 0; i < iters; i++) {
                    assertTrue(committed.contains(String.valueOf(100 + i)), "missing " + (100 + i));
                }
                cluster.checkLogsIdentical();
                return;
            }
            throw new AssertionError("term changed too often");
        }
    }

    @Test
    void partitionedLeaderRejoins() {
        try (SimCluster cluster = new SimCluster(3)) {
            cluster.one("101", 3, true);

            // leader1 is cut off and accepts entries that can never commit
            int leader1 = cluster.checkOneLeader();
            cluster.disconnect(leader1);
            cluster.start(leader1, "102");
            cluster.start(leader1, "103");
            cluster.start(leader1, "104");

            // new leader commits at the same indexes
            cluster.one("103", 2, true);
            int leader2 = cluster.checkOneLeader();
            cluster.disconnect(leader2);

            // old leader comes back: its uncommitted entries must be overwritten
            cluster.connect(leader1);
            cluster.one("104", 2, true);

            cluster.connect(leader2);
            cluster.one("105", 3, true);
            cluster.checkLogsIdentical();
        }
    }

    /** Leader backs up quickly over incorrect follower logs (accelerated log backtracking). */
    @Test
    void leaderBacksUpOverDivergentLogs() {
        try (SimCluster cluster = new SimCluster(5)) {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            cluster.one("start", 5, true);

            // put leader and one follower in a partition
            int leader1 = cluster.checkOneLeader();
            cluster.disconnect((leader1 + 2) % 5);
            cluster.disconnect((leader1 + 3) % 5);
            cluster.disconnect((leader1 + 4) % 5);

            // lots of commands that won't commit
            for (int i = 0; i < 50; i++) {
                cluster.start(leader1, "lost-" + random.nextInt());
            }
            sleep(ELECTION_TIMEOUT_MS / 2);
            cluster.disconnect(leader1);
            cluster.disconnect((leader1 + 1) % 5);

            // allow the other partition to recover and commit
            cluster.connect((leader1 + 2) % 5);
            cluster.connect((leader1 + 3) % 5);
            cluster.connect((leader1 + 4) % 5);
            for (int i = 0; i < 50; i++) {
                cluster.one("b-" + i, 3, true);
            }

            // now another partitioned leader and one follower
            int leader2 = cluster.checkOneLeader();
            int other = (leader1 + 2) % 5;
            if (leader2 == other) {
                other = (leader2 + 1) % 5;
            }
            cluster.disconnect(other);
            for (int i = 0; i < 50; i++) {
                cluster.start(leader2, "lost2-" + random.nextInt());
            }
            sleep(ELECTION_TIMEOUT_MS / 2);

            // bring the original leader back to life
            for (int i = 0; i < 5; i++) {
                cluster.disconnect(i);
            }
            cluster.connect(leader1);
            cluster.connect((leader1 + 1) % 5);
            cluster.connect(other);
            for (int i = 0; i < 50; i++) {
                cluster.one("c-" + i, 3, true);
            }

            // everyone back
            for (int i = 0; i < 5; i++) {
                cluster.connect(i);
            }
            cluster.one("end", 5, true);
            cluster.checkLogsIdentical();
        }
    }

    @Test
    void idleClusterSendsOnlyHeartbeats() {
        try (SimCluster cluster = new SimCluster(3)) {
            cluster.checkOneLeader();
            long before = cluster.network().rpcCount();
            sleep(1000);
            long rpcs = cluster.network().rpcCount() - before;
            // 2 peers x 1000 ms / 40 ms heartbeat = 50, with slack; an election storm or busy loop would be far higher
            assertTrue(rpcs <= 80, "too many RPCs while idle: " + rpcs);
        }
    }

    @Test
    void agreementOverUnreliableNetwork() throws Exception {
        try (SimCluster cluster = new SimCluster(5)) {
            cluster.network().setReliable(false);
            List<Thread> threads = new ArrayList<>();
            for (int iteration = 1; iteration < 30; iteration++) {
                for (int j = 0; j < 4; j++) {
                    String command = (100 * iteration) + "-" + j;
                    threads.add(Thread.ofVirtual().start(() -> cluster.one(command, 1, true)));
                }
                cluster.one(String.valueOf(iteration), 1, true);
            }
            cluster.network().setReliable(true);
            for (Thread t : threads) {
                t.join();
            }
            cluster.one("final", 5, true);
            cluster.checkLogsIdentical();
        }
    }

    @Test
    void followersRejectProposals() {
        try (SimCluster cluster = new SimCluster(3)) {
            int leader = cluster.checkOneLeader();
            assertNull(cluster.start((leader + 1) % 3, "x"));
            NotLeaderException e = org.junit.jupiter.api.Assertions.assertThrows(NotLeaderException.class,
                    () -> cluster.node((leader + 1) % 3).propose("x".getBytes()));
            assertEquals(leader, e.leaderHint());
        }
    }

    @Test
    void proposalFutureCompletesWithStateMachineResult() throws Exception {
        try (SimCluster cluster = new SimCluster(3)) {
            int leader = cluster.checkOneLeader();
            Proposal p = cluster.node(leader).propose("hello".getBytes());
            assertEquals("hello", new String(p.result().get(5, java.util.concurrent.TimeUnit.SECONDS)));
        }
    }
}
