package io.raftkv.kv;

import com.google.protobuf.InvalidProtocolBufferException;
import io.raftkv.proto.KvCommand;
import io.raftkv.proto.KvResult;
import io.raftkv.proto.KvSession;
import io.raftkv.proto.KvSnapshot;
import io.raftkv.raft.StateMachine;

import java.util.HashMap;
import java.util.Map;

/**
 * The replicated key-value map.
 *
 * <p><b>Exactly-once.</b> A client retries a command until it gets an answer, so the same command can end up in the
 * log more than once (for example: the leader commits it, then crashes before replying, and the client retries at
 * the next leader). Each client numbers its commands with an increasing {@code seq} and has at most one outstanding,
 * so the state machine only needs to remember, per client, the last seq it executed and that command's result. A
 * command with {@code seq <= lastSeq} is a duplicate: it is not executed again, and the remembered result is
 * returned. The table is part of the replicated state, so every node deduplicates identically, and it goes into
 * snapshots (Phase 5).
 */
public final class KvStateMachine implements StateMachine {

    private final Map<String, String> data = new HashMap<>();
    private final Map<Long, Session> sessions = new HashMap<>();

    private record Session(long lastSeq, KvResult lastResult) {
    }

    @Override
    public synchronized byte[] apply(long index, byte[] command) {
        KvCommand cmd;
        try {
            cmd = KvCommand.parseFrom(command);
        } catch (InvalidProtocolBufferException e) {
            // Deterministic on every node: a malformed command is a no-op with an empty result.
            return KvResult.getDefaultInstance().toByteArray();
        }
        Session session = sessions.get(cmd.getClientId());
        if (session != null && cmd.getSeq() <= session.lastSeq()) {
            return session.lastResult().toByteArray();
        }
        KvResult result = execute(cmd);
        sessions.put(cmd.getClientId(), new Session(cmd.getSeq(), result));
        return result.toByteArray();
    }

    private KvResult execute(KvCommand cmd) {
        return switch (cmd.getOp()) {
            case GET -> {
                String value = data.get(cmd.getKey());
                yield value == null
                        ? KvResult.newBuilder().setFound(false).build()
                        : KvResult.newBuilder().setFound(true).setValue(value).build();
            }
            case PUT -> KvResult.newBuilder().setFound(data.put(cmd.getKey(), cmd.getValue()) != null).build();
            case DELETE -> KvResult.newBuilder().setFound(data.remove(cmd.getKey()) != null).build();
            case UNRECOGNIZED -> KvResult.getDefaultInstance();
        };
    }

    @Override
    public synchronized byte[] snapshot() {
        KvSnapshot.Builder snapshot = KvSnapshot.newBuilder().putAllData(data);
        sessions.forEach((clientId, session) -> snapshot.addSessions(KvSession.newBuilder()
                .setClientId(clientId)
                .setLastSeq(session.lastSeq())
                .setLastResult(session.lastResult())));
        return snapshot.build().toByteArray();
    }

    @Override
    public synchronized void restore(byte[] bytes) {
        KvSnapshot snapshot;
        try {
            snapshot = KvSnapshot.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("corrupt KV snapshot", e);
        }
        data.clear();
        data.putAll(snapshot.getDataMap());
        sessions.clear();
        for (KvSession s : snapshot.getSessionsList()) {
            sessions.put(s.getClientId(), new Session(s.getLastSeq(), s.getLastResult()));
        }
    }

    public synchronized int size() {
        return data.size();
    }

    public synchronized String peek(String key) {
        return data.get(key);
    }

    public synchronized int sessionCount() {
        return sessions.size();
    }
}
