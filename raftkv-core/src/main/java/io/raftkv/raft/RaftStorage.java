package io.raftkv.raft;

/**
 * Durable storage for a Raft node. Every write method must be durable (on stable storage) when it returns,
 * because the node replies to RPCs right after.
 */
public interface RaftStorage extends AutoCloseable {

    HardState loadHardState();

    void saveHardState(HardState state);

    @Override
    void close();
}
