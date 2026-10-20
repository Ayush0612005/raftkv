package io.raftkv.raft;

/**
 * Storage kept in memory. It survives a node object being closed and recreated (like the MIT labs' Persister),
 * which is what the simulated-crash tests need; it does not survive the JVM.
 */
public final class MemoryStorage implements RaftStorage {

    private HardState hardState = HardState.INITIAL;

    @Override
    public synchronized HardState loadHardState() {
        return hardState;
    }

    @Override
    public synchronized void saveHardState(HardState state) {
        hardState = state;
    }

    @Override
    public void close() {
        // nothing to release; the contents stay for the next node instance
    }
}
