# Phase 5 — Snapshots (log compaction + InstallSnapshot)

**Gate:** log size bounded. **Result: passed.** In `walSizeStaysBoundedOnDisk`, 5 clients write 2,000 values of
100 bytes through the real client library onto real WALs, with a snapshot every 100 entries. The WAL never grew past
**15.4 KB**; without compaction it would pass 300 KB and keep growing. The in-memory log peaked at **105 entries**.
Each node compacted about 20 times, and the whole cluster then restarted from snapshot + WAL with all data intact.
Lagging, crashed and restarted nodes catch up through InstallSnapshot, in the simulator and over real gRPC. All
earlier gates still pass, now with snapshots on, including linearizability (a snapshot every 25 entries).

| Run (one local run) | Result |
| --- | --- |
| Entries applied | 2,001 |
| Max WAL size (any node, sampled every 5 ms) | 15,343–15,414 bytes |
| Max in-memory log | 105 entries (threshold 100) |
| Compactions | 59–60 (about 20 per node) |

## What was built

| File | Role |
| --- | --- |
| `raft.proto` | `InstallSnapshot` RPC (term, leader, last included index/term, data) |
| `Snapshot` | `(lastIncludedIndex, lastIncludedTerm, data)` |
| `StateMachine` | `snapshot()` / `restore(bytes)`, called on the applier thread, never racing `apply` |
| `KvStateMachine` | Snapshot = the map **and the dedup table** (`KvSnapshot` protobuf) |
| `RaftLog` | `compactTo` (drop a prefix, keep the boundary term for log matching), `resetTo` (drop everything) |
| `RaftStorage` / `MemoryStorage` / `FileStorage` | `saveSnapshot(snapshot, entriesAfter)`; recovery starts from the snapshot |
| `RaftNode` | threshold compaction, InstallSnapshot send/receive, applier restore, recovery from snapshot |
| `RaftConfig.snapshotThreshold`, `--snapshot-threshold` | entries between snapshots (default 10,000; 0 = off) |
| `status` CLI | now shows `SNAPSHOT` and `LOG_SIZE` per node |

## How it works

**When to snapshot.** After applying a batch, the applier checks whether `snapshotThreshold` entries have been
applied since the last snapshot. If so, it serializes the state machine (outside the lock; only this thread touches
it, so the state is exactly the state at that index). It then saves the snapshot, drops the log prefix, and keeps
the remaining entries. Only applied, and therefore committed, entries ever go into a snapshot.

**On disk: two atomic steps.**
1. `snapshot.bin` (index, term, length, CRC32C, data) is written to a temp file, fsynced, and renamed into place.
2. The WAL is rewritten to hold only the hard state and the entries after the snapshot (temp file, fsync, rename).

A crash between the two leaves a new snapshot next to the old WAL. Recovery drops the WAL entries the snapshot
covers. It keeps the rest only if the WAL's entry at the snapshot index has the snapshot's term; otherwise the
suffix conflicts with the snapshot (as after an InstallSnapshot) and is discarded. Both orders are unit-tested by
writing only step 1 and reopening. A snapshot file with a bad checksum is refused rather than guessed around,
because it was written atomically, so damage means the media is corrupt.

**InstallSnapshot (§7, Figure 13).** When a follower's `nextIndex` falls at or below the leader's snapshot index,
the replicator sends the snapshot instead of AppendEntries. The follower:
- ignores it if it already has that index committed;
- otherwise keeps the entries after the snapshot point if its log has the snapshot's last entry with the same
  term, and discards its whole log if not;
- persists the snapshot, sets `commitIndex` to the snapshot index, and hands it to the applier, which replaces the
  state machine. The applier never moves backwards.

**AppendEntries below a follower's snapshot.** If `prevLogIndex` is inside the follower's snapshot, those entries
are committed and therefore match any current leader (Leader Completeness). The follower skips the covered entries
and matches at the snapshot point instead of rejecting.

**Exactly-once survives compaction.** The KV snapshot includes the per-client dedup table. Without it, a retry
that arrives after a node restored from a snapshot would execute a second time.
`retryAfterRestartFromSnapshotTakesEffectOnce` tests exactly this.

**The harness checks through snapshots.** Like the MIT 3D tester, each node's snapshot in the simulator also
carries the harness's record of which commands that node applied. A node restored from a snapshot, its own or one
installed from the leader, is still cross-checked against every other node.

## Tests

| Test | Scenario |
| --- | --- |
| `SnapshotTest.snapshotsKeepTheLogBounded` | 200 commands, threshold 20: every node compacts, ≤ 60 entries held |
| `…laggingFollowerCatchesUpViaInstallSnapshot` | disconnected follower misses 100 compacted entries, catches up by snapshot |
| `…crashedFollowerRecoversViaInstallSnapshot` | the same after a crash and restart |
| `…wholeClusterRestartsFromSnapshots` | all nodes restart from their own snapshot + WAL suffix |
| `…snapshotsUnderCrashesAndUnreliableNetwork` | MIT 3D "unreliable + crash": lossy network, crashes, disconnects |
| `…walSizeStaysBoundedOnDisk` | **gate**: real WALs, 2,000 KV writes, WAL ≤ 64 KB the whole time (measured 15.4 KB) |
| `…snapshotsOffMeansNoCompaction` | threshold 0 disables compaction |
| `KvLinearizabilityTest` (all 7) | the Phase 4 gate, now with a snapshot every 25 entries |
| `…retryAfterRestartFromSnapshotTakesEffectOnce` | dedup table travels inside snapshots |
| `GrpcClusterTest.restartedFollowerReceivesSnapshotOverGrpc` | InstallSnapshot over real gRPC + real WALs |
| `FileStorageTest` (+5) | snapshot round trip and WAL shrink, snapshot beyond the log, crash between the two steps (matching / conflicting suffix), corrupt snapshot refused |
| `RaftLogTest` (+4) | compaction keeps the boundary term, compaction to the end, reset, invalid compaction rejected |
| `KvStateMachineTest` (+1) | restore replaces state and keeps dedup |

Total: 79 tests, all green.

## Mutation checks

| Mutation | Caught by |
| --- | --- |
| M1: KV snapshot leaves out the dedup table | `KvStateMachineTest.snapshotRestoresDataAndDedupTable`, `KvLinearizabilityTest.retryAfterRestartFromSnapshotTakesEffectOnce` (*a duplicate must get the original answer*) |
| M3: follower accepts InstallSnapshot but the applier never restores the state machine | `laggingFollower…`, `crashedFollower…`, `snapshotsUnderCrashesAndUnreliableNetwork` (*server 0 and server 1 applied different logs*) |

The randomized linearizability workloads did **not** catch M1 by themselves in the run I tried. It needs a retry
whose first copy is already inside a snapshot that the retrying node restored, and random workloads rarely produce
that interleaving. This is why the deterministic end-to-end test exists. It shows a limit of randomized testing,
recorded here instead of hidden.

## Known limits

- The snapshot travels in one message (no chunking, no `offset`/`done`); fine up to the 64 MiB gRPC limit.
- The state machine is serialized while the applier waits (no copy-on-write); applying pauses briefly for a large
  state.
- Snapshot and WAL rewrite happen under the node lock, so RPC handling pauses for two fsyncs per compaction.
- Recovery reads the whole snapshot into memory.
