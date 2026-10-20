# Phase 2 — Log replication

**Gate:** logs identical on all nodes. **Result: passed.** All 10 replication tests end with
`checkLogsIdentical()` (every node applied exactly the same commands at the same indexes), and the harness
checks State Machine Safety on every apply. 3 consecutive local runs plus CI.

## What was built

| File | Role |
| --- | --- |
| `raft.proto` | `LogEntry` (term, index, type, data), full `AppendEntries` with `prev_log_*`, `leader_commit`, conflict hints |
| `RaftLog` | In-memory log, 1-based, with an index-0 sentinel; slice / truncate / term-run lookups; ready for compaction (Phase 5) |
| `RaftNode` | `propose()`, per-peer replication, log matching, commit rule, applier thread, up-to-date vote check |
| `StateMachine` | Deterministic application interface: `apply(index, command) -> result` |
| `Proposal`, `NotLeaderException` | Result of `propose`: index, term and a future completed with the state machine's result |
| test `SimCluster` | MIT-style `one()`, `nCommitted()`, `checkLogsIdentical()`, continuous apply-consistency checking |

## How it works

- **Propose → replicate → commit → apply.** `propose()` appends to the leader's log and wakes the per-peer
  replicator threads. Each replicator sends the entries its peer is missing (up to 1024 entries / 1 MiB per RPC)
  with one request in flight, and loops immediately while the peer is behind. Proposals that arrive during an
  in-flight RPC are batched into the next one.
- **Log matching (§5.3).** A follower accepts entries only if it has `prev_log_index` with `prev_log_term`. It
  truncates only where terms actually differ, so a delayed, older AppendEntries can never delete entries that a
  newer one already appended. Overwriting a committed index throws, because it would be a safety bug.
- **Accelerated backtracking.** On a mismatch the follower returns `conflict_term` and the first index of that term
  (or its log length). The leader jumps past the whole conflicting term in one round trip instead of
  decrementing `nextIndex` one entry at a time. `leaderBacksUpOverDivergentLogs` exercises this with 50-entry
  divergent suffixes.
- **Commit rule (§5.4.2).** The leader advances `commitIndex` only to an index from its *own term* that is stored on
  a majority; older entries commit indirectly. To avoid waiting for the next client command, a new leader appends
  a **no-op** entry right away (§8), which commits everything before it.
- **Election restriction (§5.4.1).** A vote is granted only if the candidate's `(lastLogTerm, lastLogIndex)` is at
  least as up-to-date as the voter's.
- **Applier.** One platform thread applies committed entries in order, outside the node lock. It then completes
  proposal futures: with the result if the applied entry has the proposal's term, otherwise with
  `NotLeaderException`, because another leader overwrote the slot. A leader that steps down fails all pending
  futures; clients retry, and Phase 4's deduplication makes that safe.

## Tests (`ReplicationTest`, `RaftLogTest`)

| Test | Scenario |
| --- | --- |
| `basicAgreement` | 3 commands committed on all 3 nodes |
| `agreementDespiteFollowerDisconnection` | progress with 2/3; the disconnected follower catches up on rejoin |
| `noAgreementWithoutQuorum` | 5 nodes, 3 disconnected: nothing commits; recovers after reconnect |
| `concurrentProposals` | 5 concurrent proposals in one term all commit |
| `partitionedLeaderRejoins` | a stale leader's uncommitted entries are overwritten when it rejoins |
| `leaderBacksUpOverDivergentLogs` | MIT "backup" test: 50-entry divergent logs from two stale leaders |
| `idleClusterSendsOnlyHeartbeats` | ≤ 80 RPCs/s for an idle 3-node cluster (no busy loops or election storms) |
| `agreementOverUnreliableNetwork` | ~120 concurrent commands with 10% request/reply loss and random delays |
| `followersRejectProposals` | `NotLeaderException` carries the leader hint |
| `proposalFutureCompletesWithStateMachineResult` | the future returns the applied result |
| `RaftLogTest` | sentinel, append invariants, truncation, slicing limits, term-run lookups |

## Known limits (addressed later)

- The log and term live in memory only; a crash loses everything (Phase 3: WAL + recovery).
- The log grows without bound (Phase 5: snapshots).
