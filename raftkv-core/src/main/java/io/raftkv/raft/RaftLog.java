package io.raftkv.raft;

import io.raftkv.proto.LogEntry;

import java.util.ArrayList;
import java.util.List;

/**
 * The in-memory Raft log. Indexes start at 1; index 0 (term 0) is the empty log's sentinel. Everything up to
 * {@link #snapshotIndex()} has been compacted away (Phase 5); until then the snapshot index is 0.
 *
 * <p>Not thread-safe: owned by {@link RaftNode} and used only under its lock.
 */
final class RaftLog {

    private long snapshotIndex;
    private long snapshotTerm;
    /** entries.get(i) has index snapshotIndex + 1 + i. */
    private final ArrayList<LogEntry> entries = new ArrayList<>();

    long snapshotIndex() {
        return snapshotIndex;
    }

    long snapshotTerm() {
        return snapshotTerm;
    }

    long lastIndex() {
        return snapshotIndex + entries.size();
    }

    long lastTerm() {
        return entries.isEmpty() ? snapshotTerm : entries.getLast().getTerm();
    }

    /** Number of entries held in memory (not compacted). */
    int size() {
        return entries.size();
    }

    /** Term of the entry at {@code index}, or -1 if the index is compacted away or beyond the end. */
    long termAt(long index) {
        if (index == snapshotIndex) {
            return snapshotTerm;
        }
        if (index < snapshotIndex || index > lastIndex()) {
            return -1;
        }
        return entries.get(offset(index)).getTerm();
    }

    LogEntry get(long index) {
        if (index <= snapshotIndex || index > lastIndex()) {
            throw new IndexOutOfBoundsException("index " + index + " not in log (" + snapshotIndex + ", " + lastIndex() + "]");
        }
        return entries.get(offset(index));
    }

    /** Entries in [from, to], at most {@code maxCount} and roughly {@code maxBytes} (always at least one). */
    List<LogEntry> slice(long from, long to, int maxCount, long maxBytes) {
        if (from <= snapshotIndex) {
            throw new IndexOutOfBoundsException("index " + from + " is compacted (snapshot at " + snapshotIndex + ")");
        }
        long end = Math.min(to, lastIndex());
        List<LogEntry> out = new ArrayList<>();
        long bytes = 0;
        for (long i = from; i <= end && out.size() < maxCount; i++) {
            LogEntry entry = entries.get(offset(i));
            bytes += entry.getSerializedSize();
            if (!out.isEmpty() && bytes > maxBytes) {
                break;
            }
            out.add(entry);
        }
        return out;
    }

    void append(LogEntry entry) {
        if (entry.getIndex() != lastIndex() + 1) {
            throw new IllegalArgumentException("appending index " + entry.getIndex() + " after " + lastIndex());
        }
        if (entry.getTerm() < lastTerm()) {
            throw new IllegalArgumentException("term going backwards: " + entry.getTerm() + " after " + lastTerm());
        }
        entries.add(entry);
    }

    /** Deletes the entry at {@code index} and everything after it. */
    void truncateFrom(long index) {
        if (index <= snapshotIndex) {
            throw new IllegalArgumentException("cannot truncate compacted index " + index);
        }
        if (index > lastIndex()) {
            return;
        }
        entries.subList(offset(index), entries.size()).clear();
    }

    /** First index of the run of entries with {@code term} that contains {@code index}. */
    long firstIndexOfTermRun(long index) {
        long term = termAt(index);
        long i = index;
        while (i - 1 > snapshotIndex && termAt(i - 1) == term) {
            i--;
        }
        return i;
    }

    /** Last index holding an entry of {@code term}, or -1 if the log has none (in memory). */
    long lastIndexOfTerm(long term) {
        for (int i = entries.size() - 1; i >= 0; i--) {
            long t = entries.get(i).getTerm();
            if (t == term) {
                return snapshotIndex + 1 + i;
            }
            if (t < term) {
                break;
            }
        }
        return -1;
    }

    private int offset(long index) {
        return (int) (index - snapshotIndex - 1);
    }
}
