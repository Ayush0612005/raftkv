# Phase 1 — Leader election

**Gate:** exactly one leader per term; a new leader is elected when the leader fails. **Result: passed** (6/6
election tests, 3 consecutive local runs, plus CI).

## What was built

| File | Role |
| --- | --- |
| `raft.proto` | `RequestVote` and `AppendEntries` (heartbeat-only for now) messages and the `Raft` gRPC service |
| `RaftNode` | Follower / candidate / leader state machine, terms, voting, randomized election timer, heartbeats |
| `RaftConfig` | Node id, peers, election timeout range (150–300 ms), heartbeat interval (40 ms), majority |
| `RaftTransport` / `RaftRpcHandler` | Outgoing / incoming RPC interfaces, so the same node runs over gRPC or the simulator |
| `RaftStorage`, `MemoryStorage`, `HardState` | Persistent `currentTerm` + `votedFor`, saved before replying (Figure 2) |
| `RaftObserver` | Hook for tests and metrics (`onBecameLeader`) |
| test `harness/SimNetwork` | In-process network with fault injection: disconnect, partitions, drops, delays, reordering |
| test `harness/SimCluster` | n-node cluster + checks: `checkOneLeader`, `checkTerms`, `checkNoLeader`, election safety |

## Design decisions

- **One lock per node, RPCs outside the lock.** All Raft state is guarded by a single `ReentrantLock`, which makes
  the invariants of Figure 2 easy to reason about. Every outgoing RPC runs on a **virtual thread** with the lock
  released, so a dead peer never stalls the node, and thousands of blocked RPCs cost almost nothing.
- **Per-peer replicator threads.** On election, the leader starts one virtual thread per peer bound to its term.
  The thread exits as soon as the node is no longer leader of that term, so a stale leader cannot keep sending
  heartbeats under an old term. Phase 2 extends exactly this loop with log entries.
- **Ticker thread for timeouts.** A platform thread checks the randomized election deadline every 5 ms; granting a
  vote or receiving a valid AppendEntries resets it (§5.2).
- **Election safety is enforced continuously,** not just sampled: every node reports `onBecameLeader(term)` to the
  harness, and two different leaders for one term fail the test even if no polling check saw both at once.

## Tests (`ElectionTest`)

| Test | Scenario |
| --- | --- |
| `initialElection` | 3 nodes elect one leader; term stays stable for 2 s without failures |
| `reElectionAfterLeaderFailure` | leader disconnected → new leader; no quorum → no leader; quorum back → leader |
| `manyElectionsWithRandomFailures` | 7 nodes, 10 rounds of disconnecting 3 random nodes |
| `minorityPartitionCannotElectLeader` | 5 nodes split 2/3: only the majority side elects a leader in a new term |
| `electionOverUnreliableNetwork` | 5 nodes with ~10% request and reply loss plus random delay |
| `singleNodeElectsItself` | a 1-node cluster is its own majority |

## Known limits (addressed later)

- The vote's up-to-date check uses `lastLogIndex = lastLogTerm = 0` because there is no log yet (Phase 2).
- Terms are kept in `MemoryStorage`; the on-disk WAL arrives in Phase 3.
- No Pre-Vote: a node rejoining after a partition bumps the term and forces one extra election. This is safe, but
  it causes a short disruption. Pre-Vote (§9.6 of Ongaro's thesis) is a possible extension.
