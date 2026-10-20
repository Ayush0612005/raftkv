package io.raftkv.client;

import io.raftkv.proto.KvCommand;
import io.raftkv.proto.KvResponse;
import io.raftkv.proto.KvResult;
import io.raftkv.proto.Op;
import io.raftkv.raft.RaftNode;
import io.raftkv.raft.RpcException;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Client library. Finds the leader (following NOT_LEADER hints, otherwise trying servers in turn) and retries
 * every operation with the same {@code (clientId, seq)} until a server confirms it, so retries are deduplicated by
 * the state machine and each operation takes effect exactly once.
 *
 * <p>One operation at a time per client (methods are synchronized); use several clients for concurrency.
 */
public final class RaftKvClient implements AutoCloseable {

    private static final int MAX_REDIRECTS_WITHOUT_BACKOFF = 3;

    private final KvEndpoint endpoint;
    private final Duration operationTimeout;
    private final List<Integer> servers;
    private final long clientId;
    private long seq;
    private int leaderGuess = RaftNode.NONE;

    public RaftKvClient(KvEndpoint endpoint, Duration operationTimeout) {
        this.endpoint = endpoint;
        this.operationTimeout = operationTimeout;
        this.servers = List.copyOf(endpoint.serverIds());
        if (servers.isEmpty()) {
            throw new IllegalArgumentException("no servers");
        }
        long id;
        do {
            id = new SecureRandom().nextLong();
        } while (id == 0);
        this.clientId = id;
    }

    /** Connects over gRPC to a cluster given as {@code id -> host:port}. */
    public static RaftKvClient connect(Map<Integer, String> cluster) {
        return new RaftKvClient(new GrpcKvEndpoint(cluster), Duration.ofSeconds(30));
    }

    public synchronized Optional<String> get(String key) {
        KvResult r = execute(Op.GET, key, "");
        return r.getFound() ? Optional.of(r.getValue()) : Optional.empty();
    }

    /** Sets {@code key}; returns whether it existed before. */
    public synchronized boolean put(String key, String value) {
        return execute(Op.PUT, key, value).getFound();
    }

    /** Removes {@code key}; returns whether it existed. */
    public synchronized boolean delete(String key) {
        return execute(Op.DELETE, key, "").getFound();
    }

    public long clientId() {
        return clientId;
    }

    private KvResult execute(Op op, String key, String value) {
        KvCommand command = KvCommand.newBuilder()
                .setOp(op)
                .setKey(key)
                .setValue(value)
                .setClientId(clientId)
                .setSeq(++seq)
                .build();
        long deadline = System.nanoTime() + operationTimeout.toNanos();
        int target = leaderGuess != RaftNode.NONE
                ? leaderGuess
                : servers.get((int) Math.floorMod(clientId, (long) servers.size()));
        int attempts = 0;
        int redirects = 0;
        while (System.nanoTime() < deadline) {
            attempts++;
            try {
                KvResponse response = endpoint.execute(target, command);
                if (response.getStatus() == KvResponse.Status.OK) {
                    leaderGuess = target;
                    return response.getResult();
                }
                int hint = response.getLeaderHint();
                if (response.getStatus() == KvResponse.Status.NOT_LEADER && hint != target
                        && servers.contains(hint) && redirects < MAX_REDIRECTS_WITHOUT_BACKOFF) {
                    target = hint;
                    redirects++;
                    continue;
                }
                // TIMEOUT, or no usable hint: this server cannot commit right now; try another
            } catch (RpcException e) {
                // unreachable or crashed: try another
            }
            leaderGuess = RaftNode.NONE;
            redirects = 0;
            target = servers.get((servers.indexOf(target) + 1) % servers.size());
            if (attempts >= servers.size()) {
                backoff(attempts);
            }
        }
        throw new KvTimeoutException(op + " " + key + " not confirmed within " + operationTimeout);
    }

    private static void backoff(int attempts) {
        try {
            Thread.sleep(Math.min(100, 10L * attempts));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KvTimeoutException("interrupted");
        }
    }

    @Override
    public void close() {
        endpoint.close();
    }
}
