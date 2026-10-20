package io.raftkv.harness;

import io.raftkv.harness.KvHistory.Kind;
import io.raftkv.harness.KvHistory.Operation;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The checker must accept every legal history and reject illegal ones; otherwise the gate tests prove nothing. */
class LinearizabilityTest {

    private static Operation get(int client, String key, String value, long call, long ret) {
        return new Operation(client, Kind.GET, key, "", value != null, value == null ? "" : value, call, ret);
    }

    private static Operation put(int client, String key, String value, boolean existed, long call, long ret) {
        return new Operation(client, Kind.PUT, key, value, existed, "", call, ret);
    }

    private static Operation delete(int client, String key, boolean existed, long call, long ret) {
        return new Operation(client, Kind.DELETE, key, "", existed, "", call, ret);
    }

    private static boolean ok(Operation... ops) {
        return Linearizability.check(List.of(ops)).ok();
    }

    @Test
    void sequentialHistoryIsLinearizable() {
        assertTrue(ok(
                get(0, "a", null, 0, 1),
                put(0, "a", "1", false, 2, 3),
                get(1, "a", "1", 4, 5),
                put(1, "a", "2", true, 6, 7),
                delete(0, "a", true, 8, 9),
                get(1, "a", null, 10, 11)));
    }

    @Test
    void staleReadIsRejected() {
        // the put finished before the get started, so the get must see it
        assertFalse(ok(
                put(0, "a", "1", false, 0, 1),
                get(1, "a", null, 2, 3)));
    }

    @Test
    void concurrentOperationsMayTakeEffectInEitherOrder() {
        // a long put overlaps two reads: before it took effect, then after
        assertTrue(ok(
                put(0, "a", "1", false, 0, 10),
                get(1, "a", null, 2, 3),
                get(1, "a", "1", 4, 5)));
    }

    @Test
    void readsMustNotGoBackInTime() {
        // once a read saw the new value, a later read cannot see the old one
        assertFalse(ok(
                put(0, "a", "1", false, 0, 10),
                get(1, "a", "1", 2, 3),
                get(1, "a", null, 4, 5)));
    }

    @Test
    void duplicateExecutionIsRejected() {
        // a retried delete executed twice would report "existed" twice
        assertFalse(ok(
                put(0, "a", "1", false, 0, 1),
                delete(0, "a", true, 2, 3),
                delete(1, "a", true, 4, 5)));
    }

    @Test
    void valueNobodyWroteIsRejected() {
        assertFalse(ok(
                put(0, "a", "1", false, 0, 1),
                get(1, "a", "7", 2, 3)));
    }

    @Test
    void keysAreCheckedIndependently() {
        assertTrue(ok(
                put(0, "a", "1", false, 0, 1),
                put(1, "b", "2", false, 0, 1),
                get(0, "b", "2", 2, 3),
                get(1, "a", "1", 2, 3)));
        assertFalse(ok(
                put(0, "a", "1", false, 0, 1),
                get(1, "b", "1", 2, 3)));
    }

    /**
     * Builds a large history from a real sequential execution where each operation's interval is stretched around
     * its linearization point, so it overlaps others. Must be accepted, and quickly.
     */
    @Test
    void largeOverlappingLegalHistoryIsAcceptedQuickly() {
        Random random = new Random(42);
        Map<String, String> model = new HashMap<>();
        List<Operation> ops = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            long point = i * 10L;
            long call = point - random.nextInt(40);
            long ret = point + random.nextInt(40);
            String key = "k" + random.nextInt(3);
            int client = random.nextInt(8);
            switch (random.nextInt(3)) {
                case 0 -> {
                    String v = model.get(key);
                    ops.add(get(client, key, v, call, ret));
                }
                case 1 -> {
                    String v = "v" + i;
                    ops.add(put(client, key, v, model.put(key, v) != null, call, ret));
                }
                default -> ops.add(delete(client, key, model.remove(key) != null, call, ret));
            }
        }
        long start = System.nanoTime();
        assertTrue(Linearizability.check(ops).ok());
        long ms = (System.nanoTime() - start) / 1_000_000;
        assertTrue(ms < 10_000, "checker too slow: " + ms + " ms");
    }
}
