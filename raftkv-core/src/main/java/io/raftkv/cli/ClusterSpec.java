package io.raftkv.cli;

import java.util.LinkedHashMap;
import java.util.Map;

/** Parses {@code 1=host:port,2=host:port,...}. */
public final class ClusterSpec {

    private ClusterSpec() {
    }

    public static Map<Integer, String> parse(String spec) {
        Map<Integer, String> cluster = new LinkedHashMap<>();
        for (String part : spec.split(",")) {
            String trimmed = part.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int eq = trimmed.indexOf('=');
            if (eq <= 0 || eq == trimmed.length() - 1) {
                throw new IllegalArgumentException("expected id=host:port, got '" + trimmed + "'");
            }
            int id = Integer.parseInt(trimmed.substring(0, eq).trim());
            if (id < 0) {
                throw new IllegalArgumentException("node ids must be >= 0, got " + id);
            }
            if (cluster.put(id, trimmed.substring(eq + 1).trim()) != null) {
                throw new IllegalArgumentException("duplicate node id " + id);
            }
        }
        if (cluster.isEmpty()) {
            throw new IllegalArgumentException("empty cluster spec");
        }
        return cluster;
    }
}
