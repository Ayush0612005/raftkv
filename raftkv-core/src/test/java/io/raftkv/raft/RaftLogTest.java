package io.raftkv.raft;

import io.raftkv.proto.LogEntry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RaftLogTest {

    private static LogEntry entry(long index, long term) {
        return LogEntry.newBuilder().setIndex(index).setTerm(term).build();
    }

    private static RaftLog logWithTerms(long... terms) {
        RaftLog log = new RaftLog();
        for (int i = 0; i < terms.length; i++) {
            log.append(entry(i + 1, terms[i]));
        }
        return log;
    }

    @Test
    void emptyLogHasSentinelAtZero() {
        RaftLog log = new RaftLog();
        assertEquals(0, log.lastIndex());
        assertEquals(0, log.lastTerm());
        assertEquals(0, log.termAt(0));
        assertEquals(-1, log.termAt(1));
    }

    @Test
    void appendRequiresConsecutiveIndexAndMonotonicTerm() {
        RaftLog log = logWithTerms(1, 1, 2);
        assertThrows(IllegalArgumentException.class, () -> log.append(entry(5, 2)));
        assertThrows(IllegalArgumentException.class, () -> log.append(entry(4, 1)));
        log.append(entry(4, 3));
        assertEquals(4, log.lastIndex());
        assertEquals(3, log.lastTerm());
    }

    @Test
    void truncateRemovesSuffix() {
        RaftLog log = logWithTerms(1, 1, 2, 2, 3);
        log.truncateFrom(3);
        assertEquals(2, log.lastIndex());
        assertEquals(1, log.lastTerm());
        log.truncateFrom(10); // beyond the end: no-op
        assertEquals(2, log.lastIndex());
    }

    @Test
    void sliceRespectsCountAndBytes() {
        RaftLog log = logWithTerms(1, 1, 1, 1, 1);
        assertEquals(3, log.slice(2, 5, 3, Long.MAX_VALUE).size());
        assertEquals(4, log.slice(2, 99, 100, Long.MAX_VALUE).size());
        assertEquals(1, log.slice(1, 5, 100, 1).size(), "always at least one entry");
        assertEquals(0, log.slice(6, 5, 100, Long.MAX_VALUE).size());
    }

    @Test
    void compactionKeepsTheSuffixAndTheBoundaryTerm() {
        RaftLog log = logWithTerms(1, 1, 2, 2, 3);
        log.compactTo(3, 2);
        assertEquals(3, log.snapshotIndex());
        assertEquals(2, log.snapshotTerm());
        assertEquals(2, log.size());
        assertEquals(5, log.lastIndex());
        assertEquals(2, log.termAt(3), "the snapshot point keeps its term for log matching");
        assertEquals(-1, log.termAt(2), "compacted entries are gone");
        assertEquals(4, log.get(4).getIndex());
        assertThrows(IndexOutOfBoundsException.class, () -> log.slice(3, 5, 10, Long.MAX_VALUE));
        log.compactTo(2, 1); // older than the current snapshot: no-op
        assertEquals(3, log.snapshotIndex());
        log.append(entry(6, 3));
        assertEquals(6, log.lastIndex());
    }

    @Test
    void compactionToTheLastEntryLeavesAnEmptyLog() {
        RaftLog log = logWithTerms(1, 2);
        log.compactTo(2, 2);
        assertEquals(0, log.size());
        assertEquals(2, log.lastIndex());
        assertEquals(2, log.lastTerm());
    }

    @Test
    void resetDiscardsEverything() {
        RaftLog log = logWithTerms(1, 1, 1);
        log.resetTo(10, 4);
        assertEquals(0, log.size());
        assertEquals(10, log.lastIndex());
        assertEquals(4, log.lastTerm());
        assertThrows(IllegalArgumentException.class, () -> log.append(entry(3, 4)));
        log.append(entry(11, 4));
        assertEquals(11, log.lastIndex());
    }

    @Test
    void compactionRejectsWrongTermOrIndex() {
        RaftLog log = logWithTerms(1, 2);
        assertThrows(IllegalArgumentException.class, () -> log.compactTo(2, 1));
        assertThrows(IllegalArgumentException.class, () -> log.compactTo(5, 2));
    }

    @Test
    void termRunLookups() {
        RaftLog log = logWithTerms(1, 1, 4, 4, 4, 5);
        assertEquals(3, log.firstIndexOfTermRun(5));
        assertEquals(1, log.firstIndexOfTermRun(2));
        assertEquals(5, log.lastIndexOfTerm(4));
        assertEquals(-1, log.lastIndexOfTerm(3));
        assertEquals(6, log.lastIndexOfTerm(5));
    }
}
