package io.raftkv.bench;

import io.raftkv.kv.KvStateMachine;
import io.raftkv.proto.KvCommand;
import io.raftkv.proto.Op;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.TimeUnit;

/**
 * The state machine side of a write and of snapshots: decoding and applying one command (including the dedup
 * check), and serializing / restoring the whole map. Shows that the state machine is not the bottleneck; consensus
 * and fsync are.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class KvStateMachineBenchmark {

    @Param({"100000"})
    public int keys;

    private KvStateMachine sm;
    private byte[] snapshot;
    private long seq;
    private long index;
    private String value;

    @Setup(Level.Trial)
    public void fill() {
        sm = new KvStateMachine();
        value = "x".repeat(100);
        for (int k = 0; k < keys; k++) {
            put("key-" + k);
        }
        snapshot = sm.snapshot();
    }

    private byte[] put(String key) {
        byte[] command = KvCommand.newBuilder().setOp(Op.PUT).setKey(key).setValue(value)
                .setClientId(7).setSeq(++seq).build().toByteArray();
        return sm.apply(++index, command);
    }

    @Benchmark
    public byte[] applyPut() {
        return put("key-" + (seq % keys));
    }

    @Benchmark
    public byte[] applyGet() {
        byte[] command = KvCommand.newBuilder().setOp(Op.GET).setKey("key-" + (seq % keys))
                .setClientId(7).setSeq(++seq).build().toByteArray();
        return sm.apply(++index, command);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public byte[] snapshotSerialize() {
        return sm.snapshot();
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public KvStateMachine snapshotRestore() {
        KvStateMachine other = new KvStateMachine();
        other.restore(snapshot);
        return other;
    }
}
