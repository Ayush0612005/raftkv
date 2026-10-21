package io.raftkv.raft;

/**
 * The state machine's state after applying every entry up to {@code lastIncludedIndex} (whose term is
 * {@code lastIncludedTerm}). It replaces that prefix of the log (§7).
 */
public record Snapshot(long lastIncludedIndex, long lastIncludedTerm, byte[] data) {
}
