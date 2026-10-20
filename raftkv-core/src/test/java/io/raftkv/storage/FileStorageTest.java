package io.raftkv.storage;

import com.google.protobuf.ByteString;
import io.raftkv.proto.LogEntry;
import io.raftkv.raft.HardState;
import io.raftkv.raft.RaftStorage.PersistentState;
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
