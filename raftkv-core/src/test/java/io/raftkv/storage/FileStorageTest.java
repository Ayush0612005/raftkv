package io.raftkv.storage;

import com.google.protobuf.ByteString;
import io.raftkv.proto.LogEntry;
import io.raftkv.raft.HardState;
import io.raftkv.raft.RaftStorage.PersistentState;
import io.raftkv.raft.Snapshot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileStorageTest {

    @TempDir
    Path dir;

    private static LogEntry entry(long index, long term, String data) {
        return LogEntry.newBuilder().setIndex(index).setTerm(term).setData(ByteString.copyFromUtf8(data)).build();
    }

    private static List<LogEntry> entries(long from, long to, long term) {
        return LongStream.rangeClosed(from, to).mapToObj(i -> entry(i, term, "v" + i)).toList();
    }

    @Test
    void emptyDirectoryRecoversInitialState() {
        try (FileStorage storage = new FileStorage(dir)) {
            PersistentState state = storage.load();
            assertEquals(HardState.INITIAL, state.hardState());
            assertTrue(state.entries().isEmpty());
        }
    }

    @Test
    void syncedStateSurvivesRestart() {
        try (FileStorage storage = new FileStorage(dir)) {
            storage.saveHardState(new HardState(3, 1));
            storage.append(entries(1, 10, 3));
            storage.sync();
        }
        try (FileStorage storage = new FileStorage(dir)) {
            PersistentState state = storage.load();
            assertEquals(new HardState(3, 1), state.hardState());
            assertEquals(entries(1, 10, 3), state.entries());
        }
    }

    @Test
    void latestHardStateWins() {
        try (FileStorage storage = new FileStorage(dir)) {
            storage.saveHardState(new HardState(1, 0));
            storage.saveHardState(new HardState(2, -1));
            storage.saveHardState(new HardState(2, 4));
        }
        try (FileStorage storage = new FileStorage(dir)) {
            assertEquals(new HardState(2, 4), storage.load().hardState());
        }
    }

    @Test
    void unsyncedWritesAreLostOnCrash() {
        try (FileStorage storage = new FileStorage(dir)) {
            storage.append(entries(1, 3, 1));
            storage.sync();
            storage.append(entries(4, 6, 1)); // never synced
        }
        try (FileStorage storage = new FileStorage(dir)) {
            assertEquals(entries(1, 3, 1), storage.load().entries());
        }
    }

    @Test
    void truncationIsReplayed() {
        try (FileStorage storage = new FileStorage(dir)) {
            storage.append(entries(1, 5, 1));
            storage.truncateFrom(3);
            storage.append(entries(3, 4, 2));
            storage.sync();
        }
        try (FileStorage storage = new FileStorage(dir)) {
            List<LogEntry> expected = new java.util.ArrayList<>(entries(1, 2, 1));
            expected.addAll(entries(3, 4, 2));
            assertEquals(expected, storage.load().entries());
        }
    }

    @Test
    void tornTailIsCutOffAndLogStaysUsable() throws IOException {
        Path wal;
        try (FileStorage storage = new FileStorage(dir)) {
            storage.append(entries(1, 5, 1));
            storage.sync();
            wal = storage.walPath();
        }
        long validSize = Files.size(wal);
        // a crash in the middle of writing the next record: header says 100 bytes, only 7 made it
        Files.write(wal, new byte[] {0, 0, 0, 100, 1, 2, 3}, StandardOpenOption.APPEND);

        try (FileStorage storage = new FileStorage(dir)) {
            assertEquals(entries(1, 5, 1), storage.load().entries());
            assertEquals(validSize, Files.size(wal), "torn tail should be truncated");
            storage.append(entries(6, 7, 1));
            storage.sync();
        }
        try (FileStorage storage = new FileStorage(dir)) {
            assertEquals(entries(1, 7, 1), storage.load().entries());
        }
    }

    @Test
    void corruptRecordEndsTheLog() throws IOException {
        Path wal;
        try (FileStorage storage = new FileStorage(dir)) {
            storage.append(entries(1, 3, 1));
            storage.sync();
            wal = storage.walPath();
        }
        byte[] bytes = Files.readAllBytes(wal);
        bytes[bytes.length - 2] ^= 0x5A; // flip bits in the last record's payload
        Files.write(wal, bytes);

        try (FileStorage storage = new FileStorage(dir)) {
            assertEquals(entries(1, 2, 1), storage.load().entries(), "checksum must reject the damaged record");
        }
    }

    // ------------------------------------------------------------------------------------------ snapshots

    private static Snapshot snapshot(long index, long term, String data) {
        return new Snapshot(index, term, data.getBytes());
    }

    @Test
    void snapshotReplacesTheLogPrefixAndShrinksTheWal() {
        long before;
        try (FileStorage storage = new FileStorage(dir)) {
            storage.saveHardState(new HardState(5, 2));
            storage.append(entries(1, 200, 5));
            storage.sync();
            before = storage.sizeBytes();
            storage.saveSnapshot(snapshot(190, 5, "state@190"), entries(191, 200, 5));
            assertTrue(storage.sizeBytes() < before / 5, "WAL should shrink: " + storage.sizeBytes() + " vs " + before);
            storage.append(entries(201, 205, 5));
            storage.sync();
        }
        try (FileStorage storage = new FileStorage(dir)) {
            PersistentState state = storage.load();
            assertEquals(new HardState(5, 2), state.hardState(), "hard state survives the WAL rewrite");
            assertEquals(190, state.snapshot().lastIncludedIndex());
            assertEquals(5, state.snapshot().lastIncludedTerm());
            assertEquals("state@190", new String(state.snapshot().data()));
            assertEquals(entries(191, 205, 5), state.entries());
        }
    }

    @Test
    void snapshotBeyondTheLogLeavesItEmpty() {
        try (FileStorage storage = new FileStorage(dir)) {
            storage.append(entries(1, 5, 1));
            storage.sync();
            storage.saveSnapshot(snapshot(50, 3, "from leader"), List.of()); // InstallSnapshot
        }
        try (FileStorage storage = new FileStorage(dir)) {
            PersistentState state = storage.load();
            assertEquals(50, state.snapshot().lastIncludedIndex());
            assertTrue(state.entries().isEmpty());
        }
    }

    /** Crash after the snapshot file was renamed into place but before the WAL was rewritten. */
    @Test
    void crashBetweenSnapshotAndWalRewriteKeepsMatchingSuffix() throws IOException {
        try (FileStorage storage = new FileStorage(dir)) {
            storage.append(entries(1, 10, 2));
            storage.sync();
        }
        FileStorage.writeSnapshotFile(dir, snapshot(6, 2, "s"), true); // step 1 only
        try (FileStorage storage = new FileStorage(dir)) {
            assertEquals(6, storage.load().snapshot().lastIncludedIndex());
            assertEquals(entries(7, 10, 2), storage.load().entries(), "entries the snapshot covers are dropped");
        }
    }

    @Test
    void crashBetweenSnapshotAndWalRewriteDropsConflictingSuffix() throws IOException {
        try (FileStorage storage = new FileStorage(dir)) {
            storage.append(entries(1, 10, 2));
            storage.sync();
        }
        // a leader's snapshot whose term at index 6 differs from ours: our entries after it conflict
        FileStorage.writeSnapshotFile(dir, snapshot(6, 3, "s"), true);
        try (FileStorage storage = new FileStorage(dir)) {
            assertTrue(storage.load().entries().isEmpty());
        }
    }

    @Test
    void corruptSnapshotIsRefused() throws IOException {
        try (FileStorage storage = new FileStorage(dir)) {
            storage.saveSnapshot(snapshot(3, 1, "important state"), List.of());
        }
        Path file = dir.resolve(FileStorage.SNAPSHOT_FILE);
        byte[] bytes = Files.readAllBytes(file);
        bytes[bytes.length - 1] ^= 0x01;
        Files.write(file, bytes);
        org.junit.jupiter.api.Assertions.assertThrows(java.io.UncheckedIOException.class, () -> new FileStorage(dir));
    }

    @Test
    void syncWithoutFsyncStillWritesToFile() {
        try (FileStorage storage = new FileStorage(dir, false)) {
            storage.append(entries(1, 2, 1));
            storage.sync();
            assertTrue(storage.sizeBytes() > 0);
        }
        try (FileStorage storage = new FileStorage(dir, false)) {
            assertEquals(2, storage.load().entries().size());
        }
    }
}
