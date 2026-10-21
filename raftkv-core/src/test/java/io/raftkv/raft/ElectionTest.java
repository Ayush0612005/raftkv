package io.raftkv.raft;

import io.raftkv.harness.SimCluster;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static io.raftkv.harness.SimCluster.ELECTION_TIMEOUT_MS;
import static io.raftkv.harness.SimCluster.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Phase 1 gate: exactly one leader; re-elects on failure. Modelled on MIT 6.5840 lab 3A. */
@Tag("fault")
class ElectionTest {

    @Test
    void initialElection() {
        try (SimCluster cluster = new SimCluster(3)) {
            cluster.checkOneLeader();

            // everyone agrees on the term
            sleep(50);
            long term1 = cluster.checkTerms();
            assertTrue(term1 >= 1, "term is " + term1 + ", but should be at least 1");

            // with no failures the leader keeps its job and the term stays the same
            sleep(2 * ELECTION_TIMEOUT_MS);
            long term2 = cluster.checkTerms();
            assertEquals(term1, term2, "term changed even though there were no failures");

            cluster.checkOneLeader();
        }
    }

    @Test
    void reElectionAfterLeaderFailure() {
        try (SimCluster cluster = new SimCluster(3)) {
            int leader1 = cluster.checkOneLeader();

            // leader disconnects: a new one is elected
            cluster.disconnect(leader1);
            int leader2 = cluster.checkOneLeader();
            assertNotEquals(leader1, leader2);

            // old leader rejoins: it must not disturb the new leader's authority for long
            cluster.connect(leader1);
            int leader3 = cluster.checkOneLeader();

            // no quorum: no leader
            cluster.disconnect(leader3);
            cluster.disconnect((leader3 + 1) % 3);
            sleep(2 * ELECTION_TIMEOUT_MS);
            cluster.checkNoLeader();

            // quorum back: a leader again
            cluster.connect((leader3 + 1) % 3);
            cluster.checkOneLeader();

            // last node rejoins: still exactly one leader
            cluster.connect(leader3);
            cluster.checkOneLeader();
        }
    }

    @Test
    void manyElectionsWithRandomFailures() {
        int n = 7;
        try (SimCluster cluster = new SimCluster(n)) {
            cluster.checkOneLeader();
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int iteration = 0; iteration < 10; iteration++) {
                // disconnect three random nodes; four remain, which is a majority of seven
                int a = random.nextInt(n);
                int b = random.nextInt(n);
                int c = random.nextInt(n);
                cluster.disconnect(a);
                cluster.disconnect(b);
                cluster.disconnect(c);
                cluster.checkOneLeader();
                cluster.connect(a);
                cluster.connect(b);
                cluster.connect(c);
            }
            cluster.checkOneLeader();
        }
    }

    @Test
    void minorityPartitionCannotElectLeader() {
        try (SimCluster cluster = new SimCluster(5)) {
            int leader = cluster.checkOneLeader();
            int other = (leader + 1) % 5;
            Set<Integer> minority = Set.of(leader, other);
            Set<Integer> majority = Set.of((leader + 2) % 5, (leader + 3) % 5, (leader + 4) % 5);
            cluster.network().partition(minority, majority);

            sleep(2 * ELECTION_TIMEOUT_MS);
            int newLeader = -1;
            for (int i : majority) {
                if (cluster.node(i).status().isLeader()) {
                    newLeader = i;
                }
            }
            assertTrue(newLeader >= 0, "majority side should have elected a leader");
            long majorityTerm = cluster.node(newLeader).status().term();
            // the old leader may still think it leads, but only in an older term; nobody in the minority wins a newer one
            for (int i : minority) {
                NodeStatus status = cluster.node(i).status();
                if (status.isLeader()) {
                    assertTrue(status.term() < majorityTerm, "minority node " + i + " leads in term " + status.term());
                }
            }

            cluster.network().heal();
            cluster.checkOneLeader();
        }
    }

    @Test
    void electionOverUnreliableNetwork() {
        try (SimCluster cluster = new SimCluster(5)) {
            cluster.network().setReliable(false);
            for (int i = 0; i < 5; i++) {
                cluster.checkOneLeader();
            }
            cluster.network().setReliable(true);
            cluster.checkOneLeader();
        }
    }

    @Test
    void singleNodeElectsItself() {
        try (SimCluster cluster = new SimCluster(1)) {
            assertEquals(0, cluster.checkOneLeader());
        }
    }
}
