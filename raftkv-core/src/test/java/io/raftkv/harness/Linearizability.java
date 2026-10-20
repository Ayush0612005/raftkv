package io.raftkv.harness;

import io.raftkv.harness.KvHistory.Operation;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Linearizability checker for KV histories: the Wing &amp; Gong search with Lowe's memoization ("WGL", as used by
 * Knossos and Porcupine).
 *
 * <p>A history is linearizable if every operation can be given a single instant between its call and its return
 * such that, in that order, the operations are a legal sequential execution of a map. Keys are independent, so the
 * history is split per key and each part is checked separately (P-compositionality): far cheaper than one search.
 *
 * <p>The search walks the events in time order. At a call event it tries to linearize that operation now (if the
 * sequential model accepts its result); at a return event whose operation is not yet linearized it must backtrack.
 * States already explored (the set of linearized operations plus the model state) are cached and skipped.
 */
public final class Linearizability {

    private Linearizability() {
    }

    /** {@code ok}, or the first key whose sub-history is not linearizable, with that sub-history. */
    public record Result(boolean ok, String key, List<Operation> keyHistory) {

        public String describe() {
            if (ok) {
                return "linearizable";
            }
            StringBuilder sb = new StringBuilder("not linearizable on key '" + key + "':\n");
            keyHistory.stream().sorted(Comparator.comparingLong(Operation::call))
                    .forEach(op -> sb.append("  ").append(op).append('\n'));
            return sb.toString();
        }
    }

    public static Result check(List<Operation> history) {
        Map<String, List<Operation>> byKey = new LinkedHashMap<>();
        for (Operation op : history) {
            byKey.computeIfAbsent(op.key(), k -> new ArrayList<>()).add(op);
        }
        for (var entry : byKey.entrySet()) {
            if (!checkKey(entry.getValue())) {
                return new Result(false, entry.getKey(), entry.getValue());
            }
        }
        return new Result(true, null, List.of());
    }

    // ------------------------------------------------------------------------------------ sequential model

    private record Step(boolean ok, String state) {
    }

    /** One key of a map; {@code state} is its value, null when absent. */
    private static Step step(String state, Operation op) {
        return switch (op.kind()) {
            case GET -> new Step(op.found() ? Objects.equals(state, op.output()) : state == null, state);
            case PUT -> new Step(op.found() == (state != null), op.input());
            case DELETE -> new Step(op.found() == (state != null), null);
        };
    }

    // ------------------------------------------------------------------------------------------ WGL search

    private static final class Event {
        final Operation op;
        final int id;
        final boolean isCall;
        final long time;
        Event match;
        Event prev;
        Event next;

        Event(Operation op, int id, boolean isCall, long time) {
            this.op = op;
            this.id = id;
            this.isCall = isCall;
            this.time = time;
        }
    }

    private record Frame(Event call, String stateBefore) {
    }

    private record CacheKey(BitSet linearized, String state) {
    }

    static boolean checkKey(List<Operation> ops) {
        List<Event> events = new ArrayList<>(ops.size() * 2);
        for (int i = 0; i < ops.size(); i++) {
            Operation op = ops.get(i);
            if (op.ret() < op.call()) {
                throw new IllegalArgumentException("operation returns before it is called: " + op);
            }
            Event call = new Event(op, i, true, op.call());
            Event ret = new Event(op, i, false, op.ret());
            call.match = ret;
            events.add(call);
            events.add(ret);
        }
        // calls before returns at equal times: overlapping is the permissive (and correct) reading of a tie
        events.sort(Comparator.comparingLong((Event e) -> e.time).thenComparing(e -> e.isCall ? 0 : 1));

        Event head = new Event(null, -1, false, Long.MIN_VALUE);
        Event last = head;
        for (Event e : events) {
            last.next = e;
            e.prev = last;
            last = e;
        }

        Set<CacheKey> cache = new HashSet<>();
        Deque<Frame> stack = new ArrayDeque<>();
        BitSet linearized = new BitSet(ops.size());
        String state = null;
        Event entry = head.next;
        while (head.next != null) {
            if (entry.isCall) {
                Step s = step(state, entry.op);
                if (s.ok()) {
                    BitSet next = (BitSet) linearized.clone();
                    next.set(entry.id);
                    if (cache.add(new CacheKey(next, s.state()))) {
                        stack.push(new Frame(entry, state));
                        state = s.state();
                        linearized = next;
                        lift(entry);
                        entry = head.next;
                        continue;
                    }
                }
                entry = entry.next;
            } else {
                // an operation returned without being linearized: undo the most recent choice
                if (stack.isEmpty()) {
                    return false;
                }
                Frame frame = stack.pop();
                state = frame.stateBefore();
                linearized = (BitSet) linearized.clone();
                linearized.clear(frame.call().id);
                unlift(frame.call());
                entry = frame.call().next;
            }
        }
        return true;
    }

    /** Removes a call and its return from the event list. */
    private static void lift(Event call) {
        unlink(call);
        unlink(call.match);
    }

    private static void unlink(Event e) {
        e.prev.next = e.next;
        if (e.next != null) {
            e.next.prev = e.prev;
        }
    }

    /** Puts a call and its return back where they were (reverse order of removal). */
    private static void unlift(Event call) {
        relink(call.match);
        relink(call);
    }

    private static void relink(Event e) {
        e.prev.next = e;
        if (e.next != null) {
            e.next.prev = e;
        }
    }
}
