package io.raftkv.raft;

/**
 * The replicated application. Raft calls {@link #apply} exactly once per committed command, in log order, from a
 * single applier thread, on every node. Implementations must be deterministic: same commands, same results.
 *
 * <p>{@link #snapshot} and {@link #restore} are called on the same applier thread, so they never race with
 * {@code apply}. The snapshot must capture <em>everything</em> that influences future results, including
 * bookkeeping such as client deduplication tables.
 */
public interface StateMachine {

    /** Applies the command at {@code index}; the returned bytes go back to the client that proposed it. */
    byte[] apply(long index, byte[] command);

    /** Serializes the current state (after the last applied command). */
    byte[] snapshot();

    /** Replaces the whole state with a snapshot produced by {@link #snapshot} (possibly on another node). */
    void restore(byte[] snapshot);
}
