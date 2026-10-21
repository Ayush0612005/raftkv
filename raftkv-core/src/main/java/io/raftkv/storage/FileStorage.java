package io.raftkv.storage;

import com.google.protobuf.InvalidProtocolBufferException;
import io.raftkv.proto.LogEntry;
import io.raftkv.raft.HardState;
import io.raftkv.raft.RaftStorage;
import io.raftkv.raft.Snapshot;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * Raft storage on disk: an append-only write-ahead log plus a snapshot file.
 *
 * <p>WAL record format (big-endian):
 * <pre>
 *   int    length   bytes of type + payload
 *   int    crc32c   over type + payload
 *   byte   type     1 = HARD_STATE (long term, int votedFor)
 *                   2 = ENTRY      (LogEntry protobuf)
 *                   3 = TRUNCATE   (long fromIndex)
 *   byte[] payload
 * </pre>
 * The log is never modified in place: truncation is itself a record, applied on replay. Writes are buffered in
 * memory and reach the file at {@link #sync()}, followed by {@code fsync} (one fsync covers every record written
 * since the last one: group commit).
 *
 * <p>Recovery reads records until the first one that is incomplete or fails its checksum. That is where a crash
 * interrupted a write (a torn write); the file is cut back to the last valid record. Nothing after a bad record was
 * ever acknowledged, because acknowledgements happen only after a successful sync.
 *
 * <p><b>Snapshots</b> ({@link #saveSnapshot}) take two steps, each made atomic by write-temp + fsync + rename:
 * (1) {@code snapshot.bin} = {@code long lastIncludedIndex, long lastIncludedTerm, int length, int crc32c, data};
 * (2) the WAL is rewritten to contain only the hard state and the entries after the snapshot. A crash between the
 * two leaves the new snapshot with the old WAL; recovery then drops WAL entries the snapshot covers, and keeps the
 * rest only if the WAL's entry at the snapshot index has the snapshot's term (otherwise that suffix conflicts with
 * the snapshot, as after an InstallSnapshot, and is discarded).
 */
public final class FileStorage implements RaftStorage {

    static final String WAL_FILE = "wal.log";
    static final String SNAPSHOT_FILE = "snapshot.bin";
    static final byte HARD_STATE = 1;
    static final byte ENTRY = 2;
    static final byte TRUNCATE = 3;
    private static final int HEADER_BYTES = 8;
    private static final int MAX_RECORD_BYTES = 64 << 20;

    private final Path dir;
    private final Path walPath;
    private final boolean fsync;
    private FileChannel channel;
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    private final PersistentState recovered;
    private HardState hardState;
    private long fileBytes;

    /**
     * Opens (or creates) the storage in {@code dir} and recovers its contents.
     *
     * @param fsync false only for tests and benchmarks that measure everything except the disk
     */
    public FileStorage(Path dir, boolean fsync) {
        this.dir = dir;
        this.fsync = fsync;
        this.walPath = dir.resolve(WAL_FILE);
        try {
            Files.createDirectories(dir);
            Files.deleteIfExists(dir.resolve(WAL_FILE + ".tmp"));
            Files.deleteIfExists(dir.resolve(SNAPSHOT_FILE + ".tmp"));
            boolean created = !Files.exists(walPath);
            this.channel = openWal();
            Snapshot snapshot = readSnapshot(dir.resolve(SNAPSHOT_FILE));
            Replay replay = replay(channel);
            this.hardState = replay.hardState;
            this.recovered = new PersistentState(replay.hardState, snapshot, entriesAfter(snapshot, replay.entries));
            this.fileBytes = replay.validBytes;
            if (channel.size() > fileBytes) {
                // torn or corrupt tail from a crash mid-write: drop it
                channel.truncate(fileBytes);
                channel.force(true);
            }
            channel.position(fileBytes);
            if (created) {
                syncDirectory(dir);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot open storage in " + dir, e);
        }
    }

    public FileStorage(Path dir) {
        this(dir, true);
    }

    // ------------------------------------------------------------------------------------------------ write

    @Override
    public synchronized PersistentState load() {
        return recovered;
    }

    @Override
    public synchronized void saveHardState(HardState state) {
        writeRecord(buffer, HARD_STATE, hardStatePayload(state));
        sync();
        hardState = state;
    }

    @Override
    public synchronized void append(List<LogEntry> entries) {
        for (LogEntry entry : entries) {
            writeRecord(buffer, ENTRY, entry.toByteArray());
        }
    }

    @Override
    public synchronized void truncateFrom(long index) {
        writeRecord(buffer, TRUNCATE, ByteBuffer.allocate(8).putLong(index).array());
    }

    @Override
    public synchronized void sync() {
        try {
            if (buffer.size() > 0) {
                ByteBuffer data = ByteBuffer.wrap(buffer.toByteArray());
                while (data.hasRemaining()) {
                    channel.write(data);
                }
                fileBytes += buffer.size();
                buffer.reset();
            }
            if (fsync) {
                channel.force(false);
            }
        } catch (IOException e) {
            // A failed fsync leaves the file in an unknown state; the only safe reaction is to stop the node.
            throw new UncheckedIOException("WAL sync failed", e);
        }
    }

    @Override
    public synchronized void saveSnapshot(Snapshot snapshot, List<LogEntry> entriesAfter) {
        try {
            // 1. the snapshot, atomically
            writeSnapshotFile(dir, snapshot, fsync);

            // 2. a fresh WAL holding only what the snapshot does not cover, swapped in atomically
            ByteArrayOutputStream fresh = new ByteArrayOutputStream();
            writeRecord(fresh, HARD_STATE, hardStatePayload(hardState));
            for (LogEntry entry : entriesAfter) {
                writeRecord(fresh, ENTRY, entry.toByteArray());
            }
            Path tmp = dir.resolve(WAL_FILE + ".tmp");
            writeFile(tmp, fresh.toByteArray(), fsync);
            channel.close();
            Files.move(tmp, walPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            syncDirectory(dir);

            buffer.reset(); // superseded: entriesAfter is the complete log after the snapshot
            channel = openWal();
            fileBytes = channel.size();
            channel.position(fileBytes);
        } catch (IOException e) {
            throw new UncheckedIOException("saving snapshot failed", e);
        }
    }

    /** Bytes in the log file (synced records only). */
    public synchronized long sizeBytes() {
        return fileBytes;
    }

    @Override
    public synchronized void close() {
        buffer.reset(); // unsynced writes are lost, as in a crash
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private FileChannel openWal() throws IOException {
        return FileChannel.open(walPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    private static byte[] hardStatePayload(HardState state) {
        return ByteBuffer.allocate(12).putLong(state.term()).putInt(state.votedFor()).array();
    }

    private static void writeRecord(ByteArrayOutputStream out, byte type, byte[] payload) {
        CRC32C crc = new CRC32C();
        crc.update(type);
        crc.update(payload);
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES + 1)
                .putInt(1 + payload.length)
                .putInt((int) crc.getValue())
                .put(type);
        out.writeBytes(header.array());
        out.writeBytes(payload);
    }

    // -------------------------------------------------------------------------------------------- snapshot file

    static void writeSnapshotFile(Path dir, Snapshot snapshot, boolean fsync) throws IOException {
        CRC32C crc = new CRC32C();
        crc.update(snapshot.data());
        ByteBuffer file = ByteBuffer.allocate(24 + snapshot.data().length)
                .putLong(snapshot.lastIncludedIndex())
                .putLong(snapshot.lastIncludedTerm())
                .putInt(snapshot.data().length)
                .putInt((int) crc.getValue())
                .put(snapshot.data());
        Path tmp = dir.resolve(SNAPSHOT_FILE + ".tmp");
        writeFile(tmp, file.array(), fsync);
        Files.move(tmp, dir.resolve(SNAPSHOT_FILE), StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE);
        syncDirectory(dir);
    }

    private static Snapshot readSnapshot(Path path) throws IOException {
        if (!Files.exists(path)) {
            return null;
        }
        ByteBuffer file = ByteBuffer.wrap(Files.readAllBytes(path));
        if (file.remaining() < 24) {
            throw new IOException("snapshot file " + path + " is truncated");
        }
        long index = file.getLong();
        long term = file.getLong();
        int length = file.getInt();
        int storedCrc = file.getInt();
        if (length != file.remaining()) {
            throw new IOException("snapshot file " + path + " has the wrong length");
        }
        byte[] data = new byte[length];
        file.get(data);
        CRC32C crc = new CRC32C();
        crc.update(data);
        if ((int) crc.getValue() != storedCrc) {
            // written atomically, so this is media corruption, not a torn write: refuse to guess
            throw new IOException("snapshot file " + path + " fails its checksum");
        }
        return new Snapshot(index, term, data);
    }

    private static void writeFile(Path path, byte[] bytes, boolean fsync) throws IOException {
        try (FileChannel out = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer data = ByteBuffer.wrap(bytes);
            while (data.hasRemaining()) {
                out.write(data);
            }
            if (fsync) {
                out.force(true);
            }
        }
    }

    // --------------------------------------------------------------------------------------------- recovery

    private record Replay(HardState hardState, List<LogEntry> entries, long validBytes) {
    }

    private static Replay replay(FileChannel channel) throws IOException {
        long size = channel.size();
        if (size > Integer.MAX_VALUE) {
            throw new IOException("WAL larger than 2 GiB; snapshots should have compacted it");
        }
        ByteBuffer data = ByteBuffer.allocate((int) size);
        channel.position(0);
        while (data.hasRemaining() && channel.read(data) >= 0) {
            // read the whole file
        }
        data.flip();

        HardState hardState = HardState.INITIAL;
        List<LogEntry> entries = new ArrayList<>();
        long valid = 0;
        while (data.remaining() >= HEADER_BYTES) {
            int start = data.position();
            int length = data.getInt();
            int storedCrc = data.getInt();
            if (length < 1 || length > MAX_RECORD_BYTES || length > data.remaining()) {
                break; // incomplete record
            }
            byte[] body = new byte[length];
            data.get(body);
            CRC32C crc = new CRC32C();
            crc.update(body);
            if ((int) crc.getValue() != storedCrc) {
                break; // torn or corrupt record
            }
            ByteBuffer payload = ByteBuffer.wrap(body, 1, length - 1);
            switch (body[0]) {
                case HARD_STATE -> hardState = new HardState(payload.getLong(), payload.getInt());
                case ENTRY -> appendRecovered(entries, parseEntry(payload));
                case TRUNCATE -> {
                    long from = payload.getLong();
                    entries.removeIf(e -> e.getIndex() >= from);
                }
                default -> throw new IOException("unknown WAL record type " + body[0] + " at offset " + start);
            }
            valid = data.position();
        }
        return new Replay(hardState, entries, valid);
    }

    /** The WAL entries that belong after {@code snapshot} (see the class comment). */
    private static List<LogEntry> entriesAfter(Snapshot snapshot, List<LogEntry> walEntries) throws IOException {
        if (snapshot == null || walEntries.isEmpty()) {
            return List.copyOf(walEntries);
        }
        long si = snapshot.lastIncludedIndex();
        long first = walEntries.getFirst().getIndex();
        if (first > si) {
            if (first != si + 1) {
                throw new IOException("WAL starts at " + first + " but the snapshot ends at " + si);
            }
            return List.copyOf(walEntries); // the WAL was rewritten after this snapshot
        }
        // old WAL next to a newer snapshot: crash between the two steps of saveSnapshot
        long last = walEntries.getLast().getIndex();
        if (si > last) {
            return List.of();
        }
        LogEntry atSnapshot = walEntries.get((int) (si - first));
        if (atSnapshot.getTerm() != snapshot.lastIncludedTerm()) {
            return List.of();
        }
        return List.copyOf(walEntries.subList((int) (si - first) + 1, walEntries.size()));
    }

    private static LogEntry parseEntry(ByteBuffer payload) throws IOException {
        try {
            return LogEntry.parseFrom(payload);
        } catch (InvalidProtocolBufferException e) {
            throw new IOException("checksummed WAL entry does not parse", e);
        }
    }

    private static void appendRecovered(List<LogEntry> entries, LogEntry entry) throws IOException {
        if (!entries.isEmpty() && entry.getIndex() != entries.getLast().getIndex() + 1) {
            throw new IOException("WAL gap: entry " + entry.getIndex() + " after " + entries.getLast().getIndex());
        }
        entries.add(entry);
    }

    /** Makes renames and newly created files durable in their directory (POSIX); a no-op on Windows. */
    private static void syncDirectory(Path dir) {
        try (FileChannel d = FileChannel.open(dir, StandardOpenOption.READ)) {
            d.force(true);
        } catch (IOException | UnsupportedOperationException e) {
            // best effort
        }
    }

    Path walPath() {
        return walPath;
    }
}
