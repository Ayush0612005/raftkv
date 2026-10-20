package io.raftkv.raft;

/**
 * The replicated application. Raft calls {@link #apply} exactly once per committed command, in log order, from a
 * single applier thread, on every node. Implementations must be deterministic: same commands, same results.
 */
public interface StateMachine {

    /** Applies the command at {@code index}; the returned bytes go back to the client that proposed it. */
    byte[] apply(long index, byte[] command);
}
