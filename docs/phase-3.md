# Phase 3 — Persistence (write-ahead log + crash recovery)

**Gate:** a restarted node catches up. **Result: passed.** `restartedNodeCatchesUpFromRealWal`: a follower
crashes, 100 entries commit without it, and it restarts from its on-disk WAL and converges to an identical log.
`wholeClusterRestartKeepsCommittedEntries`: all 3 nodes crash and restart, and committed entries are still at the
same indexes. The MIT 3C-style crash tests (Figure 8, unreliable churn) pass as well.

## What was built

| File | Role |
| --- | --- |
| `storage/FileStorage` | The on-disk WAL: append-only, CRC32C per record, group-commit `fsync`, torn-tail recovery |
| `RaftStorage` | `load`, `saveHardState` (durable), `append` / `truncateFrom` (buffered), `sync`, `close` (= crash) |
| `MemoryStorage` | Models a disk with a write cache: unsynced writes are **lost on crash**, so tests catch missing syncs |
| `RaftNode` | Recovers term, vote and log on start; fsync before acknowledging; leader counts itself only when durable |
| test `SimCluster` | `crash(i)` / `restart(i)`, in-memory or real-WAL clusters |

## WAL format

```
int    length   (type + payload)
int    crc32c   (type + payload)
byte   type     1 HARD_STATE(long term, int votedFor) | 2 ENTRY(LogEntry protobuf) | 3 TRUNCATE(long fromIndex)
byte[] payload
```

- **Append-only.** Even a log truncation (a follower dropping a conflicting suffix) is a record that is applied on
  replay. Nothing is rewritten in place, so a crash can only damage the tail.
- **Recovery** replays records until the first one that is incomplete or fails its checksum, then truncates the file
  there. That is safe because nothing after the last successful `fsync` was ever acknowledged to anyone.
- **Group commit.** `append` only buffers. `sync()` writes the buffer and calls `fsync` once. The leader's
  replicators call `syncLeaderLog()` before each send, so one `fsync` covers every entry proposed since the last
  one. This is what makes throughput scale with concurrent clients (measured in Phase 6).

## The two durability rules (and how the tests prove they matter)

1. **A follower fsyncs before replying `success`.** The leader counts that reply toward a majority; if the
   follower then lost the entry in a crash, a "committed" entry could exist on only a minority.
2. **The leader counts itself only up to `durableIndex`.** Its own unsynced entries do not count toward a commit.

`saveHardState` (term and vote) is always synced before the node replies to a vote or sends a request in a new
term, so a node can never vote twice in one term across a restart.

**Mutation check:** with rule 1 removed and rule 2 relaxed, `PersistenceTest` fails in 2 of 8 tests with
`safety violated: commit index=2 server=4 13 != server=2 11`. Two nodes applied different commands at the same
index, which is exactly the State Machine Safety violation those rules prevent. With the rules restored, the
suite is green.

## Tests

| Test | Scenario |
| --- | --- |
| `basicPersistence` | crash/restart all nodes, the leader, and followers; progress continues |
| `moreCrashes` | 5 nodes, 5 rounds of crashing up to 3 nodes and restarting in staggered order |
| `partitionedLeaderAndCrashedFollower` | the only up-to-date nodes crash; a lagging node must not win |
| `figure8` | 300 rounds: leader crashes right after proposing (Figure 8 of the paper) |
| `figure8Unreliable` | the same with drops, delays and long reordering (replies delayed up to 2.2 s) |
| `unreliableChurn` | 3 clients propose for 5 s while nodes crash/restart/disconnect at random |
| `restartedNodeCatchesUpFromRealWal` | gate test on the real `FileStorage` |
| `wholeClusterRestartKeepsCommittedEntries` | full-cluster restart from disk |
| `FileStorageTest` (8) | round trip, latest hard state, unsynced writes lost, truncate replay, torn tail cut off and log reusable, CRC rejects corruption |

## Known limits (addressed later)

- A restarted node re-applies its whole log from index 1, and the WAL grows forever (Phase 5: snapshots).
- Recovery reads the whole WAL into memory (fine while snapshots keep it small; limited to 2 GiB).
- Directory `fsync` after creating the WAL is best effort (it works on Linux, which is the deployment target; on
  Windows it is a no-op).
