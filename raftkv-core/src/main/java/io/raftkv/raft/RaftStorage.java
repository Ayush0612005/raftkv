package io.raftkv.raft;

import io.raftkv.proto.LogEntry;

import java.util.List;

/**
 * Durable storage for a Raft node: hard state (term, vote), the latest snapshot, and the log after it.
 *
 * <p>Log writes ({@link #append}, {@link #truncateFrom}) may be buffered; they become durable, in order, at the next
 * {@link #sync()}, {@link #saveHardState} or {@link #saveSnapshot}. A crash loses everything not yet synced, so the
 * node syncs before it tells anyone about a write: before acknowledging AppendEntries, and before counting its own
 * log toward a commit.
 */
public interface RaftStorage extends AutoCloseable {

    /** Everything recovered from stable storage. Called once, before the node starts. */
    PersistentState load();

    /** Durably records term and vote (and everything written before it). */
    void saveHardState(HardState state);

    void append(List<LogEntry> entries);

    /** Removes the entry at {@code index} and all later ones. */
    void truncateFrom(long index);

    /** Makes all earlier writes durable. */
    void sync();

    /**
     * Durably replaces the stored log with {@code snapshot} followed by {@code entriesAfter} (whose indexes start at
     * {@code snapshot.lastIncludedIndex() + 1}). This is log compaction: storage no longer holds the prefix.
     */
    void saveSnapshot(Snapshot snapshot, List<LogEntry> entriesAfter);

    /** Releases resources. Unsynced writes are dropped, exactly as in a crash. */
    @Override
    void close();

    /**
     * @param snapshot null if none was ever taken
     * @param entries  the log after the snapshot (or from index 1)
     */
    record PersistentState(HardState hardState, Snapshot snapshot, List<LogEntry> entries) {
    }
}
