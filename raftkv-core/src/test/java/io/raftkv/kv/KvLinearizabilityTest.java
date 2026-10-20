package io.raftkv.kv;

import io.raftkv.client.RaftKvClient;
import io.raftkv.harness.KvHistory;
import io.raftkv.harness.KvHistory.Kind;
import io.raftkv.harness.KvHistory.Operation;
import io.raftkv.harness.Linearizability;
import io.raftkv.harness.SimCluster;
import io.raftkv.harness.SimKvEndpoint;
import io.raftkv.proto.KvCommand;
import io.raftkv.proto.KvResult;
import io.raftkv.proto.Op;
import io.raftkv.raft.NotLeaderException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.raftkv.harness.SimCluster.ELECTION_TIMEOUT_MS;
import static io.raftkv.harness.SimCluster.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Phase 4 gate: linearizable under concurrent clients. Several real {@link RaftKvClient}s run random GET / PUT /
 * DELETE against a few hot keys while faults are injected; every operation's call and return time is recorded and
 * the whole history must pass the WGL linearizability checker. Afterwards all replicas must hold identical data.
 */
class KvLinearizabilityTest {

    private static final int KEYS = 4;

    @TempDir
    Path dataDir;

    /** Runs the workload for {@code duration} while {@code faults} runs, then heals everything and checks. */
    private static void runAndCheck(SimCluster cluster, int clients, long durationMs, Runnable faults)
            throws Exception {
        KvHistory history = new KvHistory();
        AtomicBoolean stop = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<Thread> threads = new ArrayList<>();
        for (int c = 0; c < clients; c++) {
            int clientNo = c;
            threads.add(Thread.ofVirtual().start(() -> {
                try (RaftKvClient client = new RaftKvClient(new SimKvEndpoint(cluster), Duration.ofSeconds(60))) {
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    int n = 0;
                    while (!stop.get()) {
                        String key = "k" + random.nextInt(KEYS);
                        int dice = random.nextInt(100);
                        long call = System.nanoTime();
                        Operation op;
                        if (dice < 50) {
                            Optional<String> v = client.get(key);
                            op = new Operation(clientNo, Kind.GET, key, "", v.isPresent(), v.orElse(""), call,
                                    System.nanoTime());
                        } else if (dice < 85) {
                            String value = "c" + clientNo + "-" + n++;
                            boolean existed = client.put(key, value);
                            op = new Operation(clientNo, Kind.PUT, key, value, existed, "", call, System.nanoTime());
                        } else {
                            boolean existed = client.delete(key);
                            op = new Operation(clientNo, Kind.DELETE, key, "", existed, "", call, System.nanoTime());
                        }
                        history.add(op);
                        // think time: keeps each key's history in the low thousands, which the checker can search
                        Thread.sleep(2 + random.nextInt(9));
                    }
                } catch (Throwable t) {
                    // an operation with unknown outcome would make the history unusable: fail loudly instead
                    failure.compareAndSet(null, t);
                }
            }));
        }

        Thread faultThread = Thread.ofVirtual().start(() -> {
            try {
                faults.run();
            } catch (Throwable t) {
                failure.compareAndSet(null, new AssertionError("fault injector crashed", t));
            }
        });
        sleep(durationMs);
        stop.set(true);
        faultThread.interrupt();
        faultThread.join();
        healAll(cluster);
        for (Thread t : threads) {
            t.join(TimeUnit.SECONDS.toMillis(90));
        }
        if (failure.get() != null) {
            throw new AssertionError("client failed", failure.get());
        }

        List<Operation> ops = history.operations();
        System.out.println("history: " + ops.size() + " operations from " + clients + " clients");
        assertTrue(ops.size() > 20, "workload made too little progress: " + ops.size() + " operations");
        Linearizability.Result result = Linearizability.check(ops);
        if (!result.ok()) {
            fail(result.describe());
        }

        cluster.checkLogsIdentical();
        checkReplicasIdentical(cluster);
    }

    private static void healAll(SimCluster cluster) {
        cluster.network().heal();
        cluster.network().setReliable(true);
        cluster.network().setLongReordering(false);
        for (int i = 0; i < cluster.size(); i++) {
            if (!cluster.isAlive(i)) {
                cluster.restart(i);
            }
            cluster.connect(i);
        }
    }

    private static void checkReplicasIdentical(SimCluster cluster) {
        for (int k = 0; k < KEYS; k++) {
            String key = "k" + k;
            Set<String> values = new HashSet<>();
            for (int i = 0; i < cluster.size(); i++) {
                values.add(String.valueOf(((KvStateMachine) cluster.stateMachine(i)).peek(key)));
            }
            assertEquals(1, values.size(), "replicas disagree on " + key + ": " + values);
        }
    }

    /** Sleeps unless the fault thread was told to stop; returns false then. */
    private static boolean pause(long ms) {
        try {
            Thread.sleep(ms);
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }

    // --------------------------------------------------------------------------------------------- the gate

    @Test
    void concurrentClientsReliableNetwork() throws Exception {
        try (SimCluster cluster = new SimCluster(3, null, KvStateMachine::new)) {
            runAndCheck(cluster, 5, 3_000, () -> {
            });
        }
    }

    @Test
    void concurrentClientsUnreliableNetwork() throws Exception {
        try (SimCluster cluster = new SimCluster(5, null, KvStateMachine::new)) {
            cluster.network().setReliable(false);
            runAndCheck(cluster, 5, 5_000, () -> {
            });
        }
    }

    /** The leader keeps getting cut off into a minority, where it can accept requests but never commit them. */
    @Test
    void leaderRepeatedlyPartitionedIntoMinority() throws Exception {
        try (SimCluster cluster = new SimCluster(5, null, KvStateMachine::new)) {
            runAndCheck(cluster, 5, 8_000, () -> {
                while (true) {
                    int leader = -1;
                    for (int i = 0; i < 5; i++) {
                        if (cluster.node(i).status().isLeader()) {
                            leader = i;
                        }
                    }
                    if (leader >= 0) {
                        int buddy = (leader + 1) % 5;
                        Set<Integer> minority = Set.of(leader, buddy);
                        Set<Integer> majority = new HashSet<>(Set.of(0, 1, 2, 3, 4));
                        majority.removeAll(minority);
                        cluster.network().partition(minority, majority);
                    }
                    if (!pause(ELECTION_TIMEOUT_MS)) {
                        return;
                    }
                    cluster.network().heal();
                    if (!pause(ELECTION_TIMEOUT_MS / 2)) {
                        return;
                    }
                }
            });
        }
    }

    @Test
    void crashesAndRestarts() throws Exception {
        try (SimCluster cluster = new SimCluster(5, null, KvStateMachine::new)) {
            runAndCheck(cluster, 5, 8_000, () -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                while (true) {
                    int victim = random.nextInt(5);
                    cluster.crash(victim);
                    if (!pause(random.nextLong(ELECTION_TIMEOUT_MS))) {
                        return;
                    }
                    cluster.restart(victim);
                    if (!pause(random.nextLong(ELECTION_TIMEOUT_MS / 2))) {
                        return;
                    }
                }
            });
        }
    }

    /** Everything at once: lossy, reordering network, partitions and crashes, on real WALs. */
    @Test
    void everythingAtOnceOnRealWal() throws Exception {
        try (SimCluster cluster = new SimCluster(5, dataDir, KvStateMachine::new)) {
            cluster.network().setReliable(false);
            runAndCheck(cluster, 5, 8_000, () -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                while (true) {
                    switch (random.nextInt(3)) {
                        case 0 -> {
                            int a = random.nextInt(5);
                            int b = random.nextInt(5);
                            Set<Integer> minority = new HashSet<>(List.of(a, b));
                            Set<Integer> majority = new HashSet<>(Set.of(0, 1, 2, 3, 4));
                            majority.removeAll(minority);
                            cluster.network().partition(minority, majority);
                        }
                        case 1 -> {
                            int victim = random.nextInt(5);
                            if (cluster.isAlive(victim)) {
                                cluster.crash(victim);
                                if (!pause(random.nextLong(ELECTION_TIMEOUT_MS))) {
                                    return;
                                }
                                cluster.restart(victim);
                            }
                        }
                        default -> cluster.network().heal();
                    }
                    if (!pause(random.nextLong(ELECTION_TIMEOUT_MS))) {
                        return;
                    }
                }
            });
        }
    }

    // ----------------------------------------------------------------------------------------- exactly-once

    /**
     * The same client command lands in the log twice (a retry after the first copy committed but the reply was
     * lost). The second copy must return the first copy's result and must not overwrite a later write.
     */
    @Test
    void retriedCommandTakesEffectOnceEndToEnd() throws Exception {
        try (SimCluster cluster = new SimCluster(3, null, KvStateMachine::new)) {
            int leader = cluster.checkOneLeader();
            KvCommand aPut = KvCommand.newBuilder().setOp(Op.PUT).setKey("k").setValue("from-a")
                    .setClientId(111).setSeq(1).build();
            KvCommand bPut = KvCommand.newBuilder().setOp(Op.PUT).setKey("k").setValue("from-b")
                    .setClientId(222).setSeq(1).build();
            KvResult first = propose(cluster, leader, aPut);
            propose(cluster, leader, bPut);
            KvResult retry = propose(cluster, leader, aPut);

            assertEquals(first, retry, "a duplicate must get the original answer");
            cluster.checkLogsIdentical();
            for (int i = 0; i < 3; i++) {
                assertEquals("from-b", ((KvStateMachine) cluster.stateMachine(i)).peek("k"),
                        "node " + i + ": the duplicate overwrote a later write");
            }
        }
    }

    private static KvResult propose(SimCluster cluster, int leader, KvCommand command) throws Exception {
        try {
            byte[] result = cluster.node(leader).propose(command.toByteArray()).result().get(5, TimeUnit.SECONDS);
            return KvResult.parseFrom(result);
        } catch (NotLeaderException e) {
            throw new AssertionError("leader changed during the test", e);
        }
    }
}
