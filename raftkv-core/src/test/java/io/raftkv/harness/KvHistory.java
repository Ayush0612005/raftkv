package io.raftkv.harness;

import java.util.ArrayList;
import java.util.List;

/** A concurrent history of KV operations as clients observed them: what was asked, what came back, and when. */
public final class KvHistory {

    public enum Kind { GET, PUT, DELETE }

    /**
     * One completed operation. {@code call} and {@code ret} are {@link System#nanoTime()} before the client sent it
     * and after it got the answer (all retries included). {@code found}: GET key present / PUT key existed before /
     * DELETE key existed. {@code output}: the value a GET returned.
     */
    public record Operation(int client, Kind kind, String key, String input, boolean found, String output,
                            long call, long ret) {

        @Override
        public String toString() {
            String what = switch (kind) {
                case GET -> "get(" + key + ") -> " + (found ? output : "absent");
                case PUT -> "put(" + key + ", " + input + ") existed=" + found;
                case DELETE -> "delete(" + key + ") existed=" + found;
            };
            return "c" + client + " [" + call + ", " + ret + "] " + what;
        }
    }

    private final List<Operation> operations = new ArrayList<>();

    public synchronized void add(Operation operation) {
        operations.add(operation);
    }

    public synchronized List<Operation> operations() {
        return List.copyOf(operations);
    }

    public synchronized int size() {
        return operations.size();
    }
}
