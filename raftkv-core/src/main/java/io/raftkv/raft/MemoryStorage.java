package io.raftkv.raft;

import io.raftkv.proto.LogEntry;

import java.util.ArrayList;
import java.util.List;

/**
 * Storage kept in memory that models a disk with a write cache: writes are buffered until {@link #sync()}, and
 * {@link #close()} (a crash) throws the unsynced ones away. The durable part survives a node object being closed
 * and recreated (like the MIT labs' Persister), which is what the simulated-crash tests need.
 */
public final class MemoryStorage implements RaftStorage {

    private HardState hardState = HardState.INITIAL;
    private Snapshot snapshot;
    private final List<LogEntry> durable = new ArrayList<>();
    private final List<Runnable> unsynced = new ArrayList<>();

    @Override
    public synchronized PersistentState load() {
        return new PersistentState(hardState, snapshot, List.copyOf(durable));
    }

    @Override
    public synchronized void saveHardState(HardState state) {
        sync();
        hardState = state;
    }

    @Override
    public synchronized void append(List<LogEntry> entries) {
        List<LogEntry> copy = List.copyOf(entries);
        unsynced.add(() -> durable.addAll(copy));
    }

    @Override
    public synchronized void truncateFrom(long index) {
        unsynced.add(() -> durable.removeIf(e -> e.getIndex() >= index));
    }

    @Override
    public synchronized void sync() {
        unsynced.forEach(Runnable::run);
        unsynced.clear();
    }

    @Override
    public synchronized void saveSnapshot(Snapshot snapshot, List<LogEntry> entriesAfter) {
        unsynced.clear(); // superseded: entriesAfter is the complete log after the snapshot
        this.snapshot = snapshot;
        durable.clear();
        durable.addAll(entriesAfter);
    }

    /** Number of log entries stored (durable), for tests that check the log stays bounded. */
    public synchronized int storedEntries() {
        return durable.size();
    }

    @Override
    public synchronized void close() {
        unsynced.clear();
    }
}
