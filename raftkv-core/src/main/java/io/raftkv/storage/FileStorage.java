package io.raftkv.storage;

import com.google.protobuf.InvalidProtocolBufferException;
import io.raftkv.proto.LogEntry;
import io.raftkv.raft.HardState;
import io.raftkv.raft.RaftStorage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * Raft storage on disk: a single append-only write-ahead log.
 *
 * <p>Record format (big-endian):
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
 */
public final class FileStorage implements RaftStorage {

    static final String WAL_FILE = "wal.log";
    static final byte HARD_STATE = 1;
    static final byte ENTRY = 2;
    static final byte TRUNCATE = 3;
    private static final int HEADER_BYTES = 8;
    private static final int MAX_RECORD_BYTES = 64 << 20;

    private final Path walPath;
    private final boolean fsync;
    private final FileChannel channel;
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    private final PersistentState recovered;
    private long fileBytes;

    /**
     * Opens (or creates) the log in {@code dir} and recovers its contents.
     *
     * @param fsync false only for tests and benchmarks that measure everything except the disk
     */
    public FileStorage(Path dir, boolean fsync) {
        this.fsync = fsync;
        this.walPath = dir.resolve(WAL_FILE);
        try {
            Files.createDirectories(dir);
            boolean created = !Files.exists(walPath);
            this.channel = FileChannel.open(walPath, StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            Recovery recovery = recover(channel);
            this.recovered = recovery.state;
            this.fileBytes = recovery.validBytes;
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
            throw new UncheckedIOException("cannot open WAL in " + dir, e);
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
        ByteBuffer payload = ByteBuffer.allocate(12).putLong(state.term()).putInt(state.votedFor());
        writeRecord(HARD_STATE, payload.array());
        sync();
    }

    @Override
    public synchronized void append(List<LogEntry> entries) {
        for (LogEntry entry : entries) {
            writeRecord(ENTRY, entry.toByteArray());
        }
    }

    @Override
    public synchronized void truncateFrom(long index) {
        writeRecord(TRUNCATE, ByteBuffer.allocate(8).putLong(index).array());
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

    private void writeRecord(byte type, byte[] payload) {
        CRC32C crc = new CRC32C();
        crc.update(type);
        crc.update(payload);
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES + 1)
                .putInt(1 + payload.length)
                .putInt((int) crc.getValue())
                .put(type);
        buffer.writeBytes(header.array());
        buffer.writeBytes(payload);
    }

    // --------------------------------------------------------------------------------------------- recovery

    private record Recovery(PersistentState state, long validBytes) {
    }

    private static Recovery recover(FileChannel channel) throws IOException {
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
        return new Recovery(new PersistentState(hardState, List.copyOf(entries)), valid);
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

    /** Makes a newly created file's directory entry durable (POSIX); not supported, and not needed, on Windows. */
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
