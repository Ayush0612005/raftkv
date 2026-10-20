package io.raftkv.raft;

import com.google.protobuf.ByteString;
import io.raftkv.proto.AppendEntriesRequest;
import io.raftkv.proto.AppendEntriesResponse;
import io.raftkv.proto.EntryType;
import io.raftkv.proto.LogEntry;
import io.raftkv.proto.RequestVoteRequest;
import io.raftkv.proto.RequestVoteResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One Raft server.
 *
 * <p>Concurrency model: all state is guarded by one lock. A platform "ticker" thread drives election timeouts and a
 * platform "applier" thread feeds committed entries to the state machine in order. Outgoing RPCs run on virtual
 * threads with the lock released, so a slow or dead peer never blocks the node. While leader, one virtual
 * "replicator" thread per peer sends AppendEntries and exits as soon as the node is no longer leader of the term it
 * was started for.
 */
public final class RaftNode implements RaftRpcHandler, AutoCloseable {

    public static final int NONE = -1;
    private static final long TICK_MS = 5;
    private static final int MAX_ENTRIES_PER_APPEND = 1024;
    private static final long MAX_BYTES_PER_APPEND = 1 << 20;
    private static final int MAX_ENTRIES_PER_APPLY = 1024;
    private static final System.Logger LOG = System.getLogger(RaftNode.class.getName());

    private final RaftConfig config;
    private final int id;
    private final RaftTransport transport;
    private final RaftStorage storage;
    private final StateMachine stateMachine;
    private final RaftObserver observer;

    private final ReentrantLock lock = new ReentrantLock();
    /** Signalled when replicators may have work: new entries, role/term change, close. */
    private final Condition replicatorWakeup = lock.newCondition();
    /** Signalled when commitIndex advances or the node closes. */
    private final Condition applierWakeup = lock.newCondition();

    // Persistent state (written to storage before it becomes visible to other nodes).
    private long currentTerm;
    private int votedFor;
    private final RaftLog log = new RaftLog();

    // Volatile state on all servers.
    private Role role = Role.FOLLOWER;
    private int leaderId = NONE;
    private long commitIndex;
    private long lastApplied;
    /** Highest log index known to be on stable storage. A leader counts itself toward a commit only up to here. */
    private long durableIndex;
    /** Log writes were made since the last sync. */
    private boolean dirty;
    private long electionDeadlineNanos;
    private boolean started;
    private boolean closed;
    private Thread ticker;
    private Thread applier;

    // Volatile state on leaders, reinitialized after each election.
    private final Map<Integer, Long> nextIndex = new HashMap<>();
    private final Map<Integer, Long> matchIndex = new HashMap<>();
    /** Proposals waiting for their entry to be applied, by log index. */
    private final Map<Long, PendingProposal> pending = new HashMap<>();

    private record PendingProposal(long term, CompletableFuture<byte[]> future) {
    }

    public RaftNode(RaftConfig config, RaftTransport transport, RaftStorage storage, StateMachine stateMachine,
                    RaftObserver observer) {
        this.config = config;
        this.id = config.id();
        this.transport = transport;
        this.storage = storage;
        this.stateMachine = stateMachine;
        this.observer = observer;
        RaftStorage.PersistentState state = storage.load();
        this.currentTerm = state.hardState().term();
        this.votedFor = state.hardState().votedFor();
        for (LogEntry entry : state.entries()) {
            log.append(entry);
        }
        this.durableIndex = log.lastIndex();
    }

    public RaftNode(RaftConfig config, RaftTransport transport, RaftStorage storage, StateMachine stateMachine) {
        this(config, transport, storage, stateMachine, RaftObserver.NONE);
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
        applier = Thread.ofPlatform().name("raft-" + id + "-applier").daemon().start(this::runApplier);
        ticker = Thread.ofPlatform().name("raft-" + id + "-ticker").daemon().start(this::runTicker);
    }

    public int id() {
        return id;
    }

    public NodeStatus status() {
        lock.lock();
        try {
            return new NodeStatus(id, role, currentTerm, leaderId, log.lastIndex(), commitIndex, lastApplied);
        } finally {
            lock.unlock();
        }
    }

    // ---------------------------------------------------------------------------------------------- proposals

    /**
     * Appends {@code command} to the leader's log and starts replicating it. Returns immediately; the proposal's
     * future completes once the command is committed and applied here.
     */
    public Proposal propose(byte[] command) throws NotLeaderException {
        lock.lock();
        try {
            if (closed || role != Role.LEADER) {
                throw new NotLeaderException(closed ? NONE : leaderId);
            }
            LogEntry entry = appendAsLeader(EntryType.COMMAND, ByteString.copyFrom(command));
            CompletableFuture<byte[]> future = new CompletableFuture<>();
            pending.put(entry.getIndex(), new PendingProposal(entry.getTerm(), future));
            return new Proposal(entry.getIndex(), entry.getTerm(), future);
        } finally {
            lock.unlock();
        }
    }

    private LogEntry appendAsLeader(EntryType type, ByteString data) {
        LogEntry entry = LogEntry.newBuilder()
                .setTerm(currentTerm)
                .setIndex(log.lastIndex() + 1)
                .setType(type)
                .setData(data)
                .build();
        log.append(entry);
        storage.append(List.of(entry)); // buffered: synced by the replicators (group commit)
        dirty = true;
        replicatorWakeup.signalAll();
        if (config.peers().isEmpty()) {
            syncLeaderLog(); // a single-node cluster commits right away
        }
        return entry;
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
        if (role == Role.LEADER) {
            failPending(NONE);
        }
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
                .setLastLogIndex(log.lastIndex())
                .setLastLogTerm(log.lastTerm())
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
            nextIndex.put(peer, log.lastIndex() + 1);
            matchIndex.put(peer, 0L);
        }
        // A leader may only count replicas for entries of its own term (§5.4.2). Appending a no-op right away lets
        // it commit everything earlier in the log without waiting for the next client command.
        appendAsLeader(EntryType.NOOP, ByteString.EMPTY);
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
            if (role == Role.LEADER) {
                failPending(NONE);
            }
            role = Role.FOLLOWER;
            resetElectionTimer();
            replicatorWakeup.signalAll();
        }
    }

    /** §5.4.1: grant a vote only if the candidate's log is at least as up-to-date as ours. */
    private boolean candidateLogUpToDate(RequestVoteRequest request) {
        if (request.getLastLogTerm() != log.lastTerm()) {
            return request.getLastLogTerm() > log.lastTerm();
        }
        return request.getLastLogIndex() >= log.lastIndex();
    }

    private void persistHardState() {
        storage.saveHardState(new HardState(currentTerm, votedFor)); // also syncs earlier log writes
        markSynced();
    }

    private void syncStorage() {
        storage.sync();
        markSynced();
    }

    private void markSynced() {
        dirty = false;
        durableIndex = log.lastIndex();
    }

    /**
     * Makes the leader's own new entries durable, then re-evaluates the commit index. Called by every replicator
     * before it sends, so one fsync covers all entries proposed since the last one. Lock held.
     */
    private void syncLeaderLog() {
        if (dirty) {
            syncStorage();
            advanceCommitIndex();
        }
    }

    // ------------------------------------------------------------------------------------------- replication

    private boolean isLeaderOf(long term) {
        return !closed && role == Role.LEADER && currentTerm == term;
    }

    /**
     * Per-peer replicator loop for one leadership term. Sends the entries the peer is missing (or an empty
     * heartbeat), one request in flight at a time; loops immediately while the peer is behind, otherwise waits for
     * new entries or the heartbeat interval.
     */
    private void replicate(int peer, long term) {
        long heartbeatNanos = TimeUnit.MILLISECONDS.toNanos(config.heartbeatIntervalMs());
        lock.lock();
        try {
            while (isLeaderOf(term)) {
                syncLeaderLog();
                long next = nextIndex.get(peer);
                long prevIndex = next - 1;
                AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                        .setTerm(term)
                        .setLeaderId(id)
                        .setPrevLogIndex(prevIndex)
                        .setPrevLogTerm(log.termAt(prevIndex))
                        .addAllEntries(log.slice(next, log.lastIndex(), MAX_ENTRIES_PER_APPEND, MAX_BYTES_PER_APPEND))
                        .setLeaderCommit(commitIndex)
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
                if (response != null) {
                    handleAppendEntriesResponse(peer, term, request, response);
                }
                boolean caughtUp = nextIndex.get(peer) > log.lastIndex();
                if (isLeaderOf(term) && (response == null || caughtUp)) {
                    replicatorWakeup.awaitNanos(heartbeatNanos);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }
    }

    private void handleAppendEntriesResponse(int peer, long term, AppendEntriesRequest request,
                                             AppendEntriesResponse response) {
        if (response.getTerm() > currentTerm) {
            becomeFollower(response.getTerm());
            return;
        }
        if (!isLeaderOf(term)) {
            return;
        }
        if (response.getSuccess()) {
            long match = request.getPrevLogIndex() + request.getEntriesCount();
            if (match > matchIndex.get(peer)) {
                matchIndex.put(peer, match);
                advanceCommitIndex();
            }
            nextIndex.put(peer, Math.max(nextIndex.get(peer), match + 1));
            return;
        }
        // Log-matching failure: jump back past the whole conflicting term instead of one entry per round trip.
        long next;
        if (response.getConflictTerm() == 0) {
            next = response.getConflictIndex();
        } else {
            long lastOfTerm = log.lastIndexOfTerm(response.getConflictTerm());
            next = lastOfTerm > 0 ? lastOfTerm + 1 : response.getConflictIndex();
        }
        // Never go back past what is known to match, and never forward past what we sent.
        next = Math.max(next, matchIndex.get(peer) + 1);
        next = Math.min(next, request.getPrevLogIndex());
        nextIndex.put(peer, Math.max(1, next));
    }

    /**
     * Figure 2, rules for leaders: commitIndex = the highest N stored on a majority with log[N].term == currentTerm.
     * Lock held.
     */
    private void advanceCommitIndex() {
        if (role != Role.LEADER) {
            return;
        }
        for (long n = log.lastIndex(); n > commitIndex; n--) {
            if (log.termAt(n) != currentTerm) {
                break; // earlier entries have older terms; they commit indirectly (§5.4.2)
            }
            int replicas = durableIndex >= n ? 1 : 0;
            for (long match : matchIndex.values()) {
                if (match >= n) {
                    replicas++;
                }
            }
            if (replicas >= config.majority()) {
                commitIndex = n;
                applierWakeup.signal();
                return;
            }
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
                    && (votedFor == NONE || votedFor == request.getCandidateId())
                    && candidateLogUpToDate(request);
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
            AppendEntriesResponse.Builder response = AppendEntriesResponse.newBuilder();
            if (closed || request.getTerm() < currentTerm) {
                return response.setTerm(currentTerm).setSuccess(false).build();
            }
            // A valid leader exists for this term: candidates (and stale leaders) step down.
            becomeFollower(request.getTerm());
            leaderId = request.getLeaderId();
            resetElectionTimer();
            response.setTerm(currentTerm);

            // Log matching (§5.3): our log must contain prevLogIndex with prevLogTerm.
            long prevIndex = request.getPrevLogIndex();
            if (prevIndex > log.lastIndex()) {
                return response.setSuccess(false).setConflictTerm(0).setConflictIndex(log.lastIndex() + 1).build();
            }
            long localPrevTerm = log.termAt(prevIndex);
            if (localPrevTerm != request.getPrevLogTerm()) {
                return response.setSuccess(false)
                        .setConflictTerm(localPrevTerm)
                        .setConflictIndex(log.firstIndexOfTermRun(prevIndex))
                        .build();
            }

            // Append new entries; delete a conflicting suffix only where terms differ. Entries we already have are
            // skipped, so a delayed, older AppendEntries can never truncate entries a newer one appended.
            for (LogEntry entry : request.getEntriesList()) {
                long index = entry.getIndex();
                if (index <= log.lastIndex()) {
                    if (log.termAt(index) == entry.getTerm()) {
                        continue;
                    }
                    if (index <= commitIndex) {
                        throw new IllegalStateException("node " + id + ": leader " + request.getLeaderId()
                                + " tried to overwrite committed index " + index);
                    }
                    log.truncateFrom(index);
                    storage.truncateFrom(index);
                    dirty = true;
                }
                log.append(entry);
                storage.append(List.of(entry));
                dirty = true;
            }

            if (dirty) {
                syncStorage(); // the leader may count this ack toward a commit: the entries must survive a crash
            }
            long lastNew = prevIndex + request.getEntriesCount();
            if (request.getLeaderCommit() > commitIndex) {
                commitIndex = Math.max(commitIndex, Math.min(request.getLeaderCommit(), lastNew));
                applierWakeup.signal();
            }
            return response.setSuccess(true).setMatchIndex(lastNew).build();
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------------------------------------ apply

    private void runApplier() {
        while (true) {
            List<LogEntry> batch;
            lock.lock();
            try {
                while (!closed && lastApplied >= commitIndex) {
                    applierWakeup.awaitUninterruptibly();
                }
                if (closed) {
                    return;
                }
                batch = log.slice(lastApplied + 1, commitIndex, MAX_ENTRIES_PER_APPLY, Long.MAX_VALUE);
            } finally {
                lock.unlock();
            }

            // The state machine runs outside the lock: only this thread touches it, in log order.
            byte[][] results = new byte[batch.size()][];
            for (int i = 0; i < batch.size(); i++) {
                LogEntry entry = batch.get(i);
                if (entry.getType() == EntryType.COMMAND) {
                    results[i] = stateMachine.apply(entry.getIndex(), entry.getData().toByteArray());
                }
            }

            List<Runnable> completions = new ArrayList<>();
            lock.lock();
            try {
                for (int i = 0; i < batch.size(); i++) {
                    LogEntry entry = batch.get(i);
                    lastApplied = entry.getIndex();
                    PendingProposal p = pending.remove(entry.getIndex());
                    if (p != null) {
                        byte[] result = results[i];
                        // A different entry at this index means our proposal was overwritten by another leader.
                        completions.add(p.term() == entry.getTerm()
                                ? () -> p.future().complete(result)
                                : () -> p.future().completeExceptionally(new NotLeaderException(NONE)));
                    }
                }
            } finally {
                lock.unlock();
            }
            completions.forEach(Runnable::run); // outside the lock: callbacks may call back into the node
        }
    }

    /** Fails every waiting proposal; the leader lost its job. Lock held. */
    private void failPending(int leaderHint) {
        if (pending.isEmpty()) {
            return;
        }
        List<PendingProposal> failed = new ArrayList<>(pending.values());
        pending.clear();
        // complete asynchronously so client callbacks never run under the node lock
        Thread.ofVirtual().start(() -> failed.forEach(
                p -> p.future().completeExceptionally(new NotLeaderException(leaderHint))));
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
            failPending(NONE);
            replicatorWakeup.signalAll();
            applierWakeup.signalAll();
        } finally {
            lock.unlock();
        }
        for (Thread thread : new Thread[] {ticker, applier}) {
            if (thread != null) {
                thread.interrupt();
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        storage.close();
    }
}
