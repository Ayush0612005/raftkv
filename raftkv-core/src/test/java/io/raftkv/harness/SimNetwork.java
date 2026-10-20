package io.raftkv.harness;

import io.raftkv.proto.AppendEntriesRequest;
import io.raftkv.proto.AppendEntriesResponse;
import io.raftkv.proto.RequestVoteRequest;
import io.raftkv.proto.RequestVoteResponse;
import io.raftkv.raft.RaftRpcHandler;
import io.raftkv.raft.RaftTransport;
import io.raftkv.raft.RpcException;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * In-process network for fault-injection tests. Every RPC is a direct method call on the target's handler, with
 * faults injected around it:
 * <ul>
 *   <li>disconnect a node from everyone ({@link #disconnect}),</li>
 *   <li>split nodes into partitions that cannot talk to each other ({@link #partition}),</li>
 *   <li>unreliable mode: random delay, ~10% of requests and ~10% of replies dropped,</li>
 *   <li>long reordering: some replies are held back for up to 2 s, so old replies arrive after newer ones.</li>
 * </ul>
 * An RPC to an unreachable node fails after a short random "timeout", like a real network.
 */
public final class SimNetwork {

    private final Map<Integer, RaftRpcHandler> handlers = new ConcurrentHashMap<>();
    private final Set<Integer> disconnected = ConcurrentHashMap.newKeySet();
    private volatile Map<Integer, Integer> partitionOf = Map.of();
    private volatile boolean reliable = true;
    private volatile boolean longReordering;
    private final AtomicLong rpcCount = new AtomicLong();

    public void register(int id, RaftRpcHandler handler) {
        handlers.put(id, handler);
    }

    /** The node crashed: it receives nothing until registered again. */
    public void unregister(int id) {
        handlers.remove(id);
    }

    public void disconnect(int id) {
        disconnected.add(id);
    }

    public void connect(int id) {
        disconnected.remove(id);
    }

    public boolean isConnected(int id) {
        return !disconnected.contains(id);
    }

    /** Nodes in different groups cannot talk. Nodes not listed are in their own group 0. */
    @SafeVarargs
    public final void partition(Set<Integer>... groups) {
        Map<Integer, Integer> map = new ConcurrentHashMap<>();
        for (int g = 0; g < groups.length; g++) {
            for (int node : groups[g]) {
                map.put(node, g + 1);
            }
        }
        partitionOf = map;
    }

    public void heal() {
        partitionOf = Map.of();
    }

    public void setReliable(boolean reliable) {
        this.reliable = reliable;
    }

    public void setLongReordering(boolean longReordering) {
        this.longReordering = longReordering;
    }

    public long rpcCount() {
        return rpcCount.get();
    }

    public boolean canTalk(int a, int b) {
        Map<Integer, Integer> p = partitionOf;
        return !disconnected.contains(a) && !disconnected.contains(b)
                && p.getOrDefault(a, 0).equals(p.getOrDefault(b, 0));
    }

    public RaftTransport transportFor(int from) {
        return new RaftTransport() {
            @Override
            public RequestVoteResponse requestVote(int target, RequestVoteRequest request) throws RpcException {
                return call(from, target, h -> h.handleRequestVote(request));
            }

            @Override
            public AppendEntriesResponse appendEntries(int target, AppendEntriesRequest request) throws RpcException {
                return call(from, target, h -> h.handleAppendEntries(request));
            }
        };
    }

    <T> T call(int from, int to, Function<RaftRpcHandler, T> invocation) throws RpcException {
        rpcCount.incrementAndGet();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        if (!reliable) {
            sleep(random.nextInt(27));
            if (random.nextInt(1000) < 100) {
                throw new RpcException("request dropped");
            }
        }
        RaftRpcHandler handler = handlers.get(to);
        if (handler == null || !canTalk(from, to)) {
            sleep(random.nextInt(100));
            throw new RpcException("node " + to + " unreachable from " + from);
        }
        T response = invocation.apply(handler);
        if (!canTalk(from, to) || !handlers.containsKey(from)) {
            throw new RpcException("reply lost");
        }
        if (!reliable && random.nextInt(1000) < 100) {
            throw new RpcException("reply dropped");
        }
        if (longReordering && random.nextInt(900) < 600) {
            sleep(200 + random.nextInt(1 + random.nextInt(2000)));
        }
        return response;
    }

    /**
     * A client request to server {@code to}. Clients sit outside the partitions (they can reach a minority-side
     * server, which then cannot commit), but they cannot reach a crashed or disconnected server, and in unreliable
     * mode their requests and replies are delayed and dropped like everything else. {@code invocation} returning
     * null means the server is gone.
     */
    public <T> T callFromClient(int to, Supplier<T> invocation) throws RpcException {
        rpcCount.incrementAndGet();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        if (!reliable) {
            sleep(random.nextInt(27));
            if (random.nextInt(1000) < 100) {
                throw new RpcException("client request dropped");
            }
        }
        if (!handlers.containsKey(to) || disconnected.contains(to)) {
            sleep(random.nextInt(100));
            throw new RpcException("server " + to + " unreachable");
        }
        T response = invocation.get();
        if (response == null || disconnected.contains(to)) {
            throw new RpcException("reply lost");
        }
        if (!reliable && random.nextInt(1000) < 100) {
            throw new RpcException("client reply dropped");
        }
        return response;
    }

    private static void sleep(long ms) throws RpcException {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RpcException("interrupted");
        }
    }
}
