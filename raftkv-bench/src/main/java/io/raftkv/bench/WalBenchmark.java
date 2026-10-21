package io.raftkv.bench;

import com.google.protobuf.ByteString;
import io.raftkv.proto.LogEntry;
import io.raftkv.storage.FileStorage;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The cost the WAL puts on every write: append log entries (100-byte commands) and make them durable. Measured as
 * entries per second, for one entry per sync (no batching) and 32 entries per sync (group commit), with and without
 * fsync. The gap between those numbers is why the leader batches.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class WalBenchmark {

    @Param({"true", "false"})
    public boolean fsync;

    private Path dir;
    private FileStorage storage;
    private long nextIndex = 1;
    private ByteString payload;

    @Setup(Level.Trial)
    public void open() {
        dir = BenchDirs.fresh("wal-");
        storage = new FileStorage(dir, fsync);
        payload = ByteString.copyFrom(new byte[100]);
    }

    @TearDown(Level.Trial)
    public void close() {
        storage.close();
        BenchDirs.delete(dir);
    }

    private List<LogEntry> next(int count) {
        List<LogEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            entries.add(LogEntry.newBuilder().setTerm(1).setIndex(nextIndex++).setData(payload).build());
        }
        return entries;
    }

    @Benchmark
    public void appendSync1() {
        storage.append(next(1));
        storage.sync();
    }

    @Benchmark
    @OperationsPerInvocation(32)
    public void appendSync32() {
        storage.append(next(32));
        storage.sync();
    }
}
