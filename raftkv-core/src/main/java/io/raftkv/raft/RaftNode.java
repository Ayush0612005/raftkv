package io.raftkv.raft;

import io.raftkv.proto.AppendEntriesRequest;
import io.raftkv.proto.AppendEntriesResponse;
import io.raftkv.proto.RequestVoteRequest;
import io.raftkv.proto.RequestVoteResponse;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One Raft server.
 *
 * <p>Concurrency model: all state is guarded by one lock. A platform "ticker" thread drives election timeouts.
 * Outgoing RPCs run on virtual threads with the lock released, so a slow or dead peer never blocks the node.
 * While leader, one virtual "replicator" thread per peer sends AppendEntries (heartbeats for now) and exits as soon
 * as the node is no longer leader of the term it was started for.
 */
public final class RaftNode implements RaftRpcHandler, AutoCloseable {

    public static final int NONE = -1;
    private static final long TICK_MS = 5;
    private static final System.Logger LOG = System.getLogger(RaftNode.class.getName());

    private final RaftConfig config;
    private final int id;
    private final RaftTransport transport;
    private final RaftStorage storage;
    private final RaftObserver observer;

    private final ReentrantLock lock = new ReentrantLock();
    /** Signalled whenever replicators may have work: role/term change, close. */
    private final Condition replicatorWakeup = lock.newCondition();

    // Persistent state (written to storage before it becomes visible to other nodes).
    private long currentTerm;
    private int votedFor;

    // Volatile state.
    private Role role = Role.FOLLOWER;
    private int leaderId = NONE;
    private long electionDeadlineNanos;
    private boolean started;
    private boolean closed;
    private Thread ticker;

    public RaftNode(RaftConfig config, RaftTransport transport, RaftStorage storage, RaftObserver observer) {
        this.config = config;
        this.id = config.id();
        this.transport = transport;
        this.storage = storage;
        this.observer = observer;
        HardState hs = storage.loadHardState();
        this.currentTerm = hs.term();
        this.votedFor = hs.votedFor();
    }

    public RaftNode(RaftConfig config, RaftTransport transport, RaftStorage storage) {
        this(config, transport, storage, RaftObserver.NONE);
    }

    public void start() {
        lock.lock();
        try {
            if (started || closed) {
                throw new IllegalStateException("node " + id + " already started or closed");
            }
            started = true;
            resetElectionTimer();
        } finally {
            lock.unlock();
        }
        ticker = Thread.ofPlatform().name("raft-" + id + "-ticker").daemon().start(this::runTicker);
    }

    public int id() {
        return id;
    }

    public NodeStatus status() {
        lock.lock();
        try {
            return new NodeStatus(id, role, currentTerm, leaderId);
        } finally {
            lock.unlock();
        }
    }

    // ---------------------------------------------------------------------------------------------- election

    private void runTicker() {
        while (true) {
            lock.lock();
            try {
                if (closed) {
                    return;
                }
                if (role != Role.LEADER && System.nanoTime() - electionDeadlineNanos >= 0) {
                    startElection();
                }
            } finally {
                lock.unlock();
            }
            try {
                Thread.sleep(TICK_MS);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private void resetElectionTimer() {
        long timeoutMs = ThreadLocalRandom.current()
                .nextLong(config.electionTimeoutMinMs(), config.electionTimeoutMaxMs());
        electionDeadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    }

    /** §5.2: increment term, vote for self, reset the timer, ask everyone else for a vote. Lock held. */
    private void startElection() {
        currentTerm++;
        role = Role.CANDIDATE;
        votedFor = id;
        leaderId = NONE;
        persistHardState();
        resetElectionTimer();
        long term = currentTerm;
        LOG.log(System.Logger.Level.DEBUG, "node {0} starts election for term {1}", id, term);

        if (config.majority() == 1) {
            becomeLeader();
            return;
        }
        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                .setTerm(term)
                .setCandidateId(id)
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build();
        int[] votes = {1};
        for (int peer : config.peers()) {
            Thread.ofVirtual().name("raft-" + id + "-vote-" + peer).start(() -> {
                RequestVoteResponse response;
                try {
                    response = transport.requestVote(peer, request);
                } catch (RpcException e) {
                    return;
                }
                lock.lock();
                try {
                    if (closed) {
                        return;
                    }
                    if (response.getTerm() > currentTerm) {
                        becomeFollower(response.getTerm());
                        return;
                    }
                    if (role == Role.CANDIDATE && currentTerm == term && response.getVoteGranted()) {
                        votes[0]++;
                        if (votes[0] == config.majority()) {
                            becomeLeader();
                        }
                    }
                } finally {
                    lock.unlock();
                }
            });
        }
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = id;
        long term = currentTerm;
        LOG.log(System.Logger.Level.INFO, "node {0} is leader for term {1}", id, term);
        observer.onBecameLeader(id, term);
        for (int peer : config.peers()) {
            Thread.ofVirtual().name("raft-" + id + "-repl-" + peer).start(() -> replicate(peer, term));
        }
    }

    /**
     * Steps down to follower. If {@code term} is newer, adopts it and forgets the vote (§5.1: any RPC with a higher
     * term converts the receiver to follower). Lock held.
     */
    private void becomeFollower(long term) {
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = NONE;
            leaderId = NONE;
            persistHardState();
        }
        if (role != Role.FOLLOWER) {
            role = Role.FOLLOWER;
            resetElectionTimer();
            replicatorWakeup.signalAll();
        }
    }

    private void persistHardState() {
        storage.saveHardState(new HardState(currentTerm, votedFor));
    }

    // ------------------------------------------------------------------------------------------- replication

    private boolean isLeaderOf(long term) {
        return !closed && role == Role.LEADER && currentTerm == term;
    }

    /** Per-peer replicator loop for one leadership term. */
    private void replicate(int peer, long term) {
        long heartbeatNanos = TimeUnit.MILLISECONDS.toNanos(config.heartbeatIntervalMs());
        lock.lock();
        try {
            while (isLeaderOf(term)) {
                AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                        .setTerm(term)
                        .setLeaderId(id)
                        .build();
                AppendEntriesResponse response = null;
                lock.unlock();
                try {
                    response = transport.appendEntries(peer, request);
                } catch (RpcException e) {
                    // peer unreachable: retry at the next heartbeat
                } finally {
                    lock.lock();
                }
                if (response != null && response.getTerm() > currentTerm) {
                    becomeFollower(response.getTerm());
                }
                if (isLeaderOf(term)) {
                    replicatorWakeup.awaitNanos(heartbeatNanos);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------------------------------ RPC handlers

    @Override
    public RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        lock.lock();
        try {
            if (closed) {
                return RequestVoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(false).build();
            }
            if (request.getTerm() > currentTerm) {
                becomeFollower(request.getTerm());
            }
            boolean grant = request.getTerm() == currentTerm
                    && (votedFor == NONE || votedFor == request.getCandidateId());
            if (grant) {
                votedFor = request.getCandidateId();
                persistHardState();
                // §5.2: granting a vote counts as hearing from a would-be leader
                resetElectionTimer();
            }
            return RequestVoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(grant).build();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        lock.lock();
        try {
            if (closed || request.getTerm() < currentTerm) {
                return AppendEntriesResponse.newBuilder().setTerm(currentTerm).setSuccess(false).build();
            }
            // A valid leader exists for this term: candidates (and stale leaders) step down.
            becomeFollower(request.getTerm());
            leaderId = request.getLeaderId();
            resetElectionTimer();
            return AppendEntriesResponse.newBuilder().setTerm(currentTerm).setSuccess(true).build();
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------------------------------------ close

    /** Stops all threads. After close the node answers nothing useful; storage is closed. */
    @Override
    public void close() {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            replicatorWakeup.signalAll();
        } finally {
            lock.unlock();
        }
        if (ticker != null) {
            ticker.interrupt();
            try {
                ticker.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        storage.close();
    }
}
