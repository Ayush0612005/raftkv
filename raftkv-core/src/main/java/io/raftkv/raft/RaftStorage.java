package io.raftkv.raft;

import io.raftkv.proto.LogEntry;

import java.util.List;

/**
 * Durable storage for a Raft node: hard state (term, vote) and the log.
 *
 * <p>Log writes ({@link #append}, {@link #truncateFrom}) may be buffered; they become durable, in order, at the next
 * {@link #sync()} or {@link #saveHardState}. A crash loses everything not yet synced, so the node syncs before it
 * tells anyone about a write: before acknowledging AppendEntries, and before counting its own log toward a commit.
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

    /** Releases resources. Unsynced writes are dropped, exactly as in a crash. */
    @Override
    void close();

    record PersistentState(HardState hardState, List<LogEntry> entries) {
    }
}
