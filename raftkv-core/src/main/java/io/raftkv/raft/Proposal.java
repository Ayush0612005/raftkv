package io.raftkv.raft;

import java.util.concurrent.CompletableFuture;

/**
 * A command accepted by the leader at {@code index} in {@code term}. {@code result} completes with the state
 * machine's output once the entry is committed and applied, or fails with {@link NotLeaderException} if the leader
 * lost its job first (the command may or may not end up committed; clients retry, and the KV layer deduplicates).
 */
public record Proposal(long index, long term, CompletableFuture<byte[]> result) {
}
