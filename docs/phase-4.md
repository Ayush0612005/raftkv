# Phase 4 — KV state machine, client API, linearizability

**Gate:** linearizable under concurrent clients. **Result: passed.** In every fault mode, 5 concurrent clients run
random GET/PUT/DELETE on 4 hot keys: reliable, lossy/reordering network, the leader repeatedly partitioned into a
minority, random crashes and restarts, and all of these at once on real WALs. Every recorded history passes the WGL
linearizability checker, and afterwards all replicas hold identical data.

## What was built

| File | Role |
| --- | --- |
| `kv.proto` | `KvCommand {op, key, value, client_id, seq}`, `KvResult`, `KvResponse {OK, NOT_LEADER + hint, TIMEOUT}`, `Kv` service (`Execute`, `Status`) |
| `kv/KvStateMachine` | The map plus the per-client **dedup table** (last seq and last result) for exactly-once |
| `kv/KvService` | Proposes a command, waits for commit + apply (2 s), maps the outcome to a response |
| `transport/GrpcTransport`, `RaftGrpcService` | Node-to-node RPCs over gRPC: one channel per peer, deadline per call |
| `transport/KvGrpcService` | The client API over gRPC; the server runs calls on **virtual threads** |
| `server/RaftKvServer` | One node as a process: WAL, state machine, one gRPC port for both services |
| `client/RaftKvClient` | Java client library: leader discovery, NOT_LEADER redirects, retries with the same `(client_id, seq)` |
| `cli/Main` | picocli CLI in one shaded jar: `server`, `get`, `put`, `delete`, `status` |
| test `harness/Linearizability` | WGL checker (Wing & Gong search + Lowe's memoization), split per key |
| test `harness/SimKvEndpoint` | Runs the *real* client library over the simulated faulty network |

## Design

**Every operation goes through the log, reads included.** A GET served from the leader's local map could be stale:
a leader cut off in a minority partition does not know it has been replaced and would return old data. Putting the
GET in the log means it is answered only after a majority accepted the leader's term at that log position.
ReadIndex / lease reads (§6.4 of Ongaro's thesis) would skip the log write, but they are an optimisation, not
needed for correctness.

**Exactly-once.** A client retries until a server confirms, so a command can be committed twice. For example, the
leader commits it and crashes before replying, and the client retries at the new leader. Each client has a random
64-bit id, numbers its commands with an increasing `seq`, and keeps at most one outstanding. The state machine
remembers, per client, the last executed `seq` and its result. A duplicate is not executed again and gets the
original answer. The table is replicated state, so every node deduplicates identically (and Phase 5 snapshots will
include it).

**Client library.** It starts with the last known leader, follows `NOT_LEADER` hints (at most 3 redirects without
backoff to avoid ping-pong during elections), and otherwise tries servers in turn with a small backoff. `TIMEOUT`
means "this server could not commit in time", typically a leader in a minority, so the client moves on. Because the
retry reuses the same `seq`, moving on is always safe.

**One code path in tests and production.** `RaftKvClient` talks to a `KvEndpoint`. In production that is gRPC; in
tests it is `SimKvEndpoint` over the fault-injecting `SimNetwork`. The linearizability tests exercise exactly the
client logic that ships.

## The linearizability checker

A history is linearizable if each operation can be assigned one instant between its call and its return such that
the operations, in that order, are a legal sequential run of a map. The checker:

1. splits the history by key (keys are independent, so each part is checked on its own: P-compositionality);
2. walks call/return events in time order. At a call, it tries to linearize that operation now, if the sequential
   model accepts its result. At a return of a not-yet-linearized operation, it backtracks;
3. caches every explored `(set of linearized ops, model state)` and never explores it twice (Lowe's optimisation).

The model is a single key's value. GET must return it, PUT/DELETE must report correctly whether the key existed,
and PUT/DELETE then change it. Because PUT and DELETE report "existed", a duplicated execution becomes visible to
the checker.

`LinearizabilityTest` shows the checker is not vacuous. It **rejects** stale reads, reads going back in time, a
delete executed twice, and a value nobody wrote. It **accepts** legal overlapping histories, including a
2,000-operation history built from a real execution with stretched intervals.

## Tests

| Test | Scenario |
| --- | --- |
| `KvLinearizabilityTest.concurrentClientsReliableNetwork` | 3 nodes, 5 clients, 3 s |
| `…UnreliableNetwork` | 5 nodes, 10% request/reply loss + delays, client requests lossy too |
| `…leaderRepeatedlyPartitionedIntoMinority` | every ~1.5 s the leader + 1 node are cut off from the other 3 |
| `…crashesAndRestarts` | a random node crashes and restarts continuously |
| `…everythingAtOnceOnRealWal` | lossy network + random partitions + crashes, `FileStorage` WALs |
| `…retriedCommandTakesEffectOnceEndToEnd` | the same command committed twice: original answer, no overwrite of a later write |
| `KvStateMachineTest` (5) | ops, dedup, duplicates never undo other clients' writes, malformed command = no-op |
| `LinearizabilityTest` (8) | checker accepts legal and rejects illegal histories |
| `GrpcClusterTest` | 3 real gRPC nodes: kill the leader, keep working, restart it from its WAL, it catches up; CLI end to end |

History sizes in one run (5 clients, 2–10 ms think time between operations): reliable 5,209 operations; unreliable
212; leader partitions 1,035; crashes 260; everything at once 865. The think time is deliberate. Without it the
in-process simulator completes ~185k operations/s, and a 3-second run yields ~140k operations *per key*, a
history the exponential-worst-case search cannot finish once it contains a violation.

**Mutation check (exactly-once):** with the dedup check in `KvStateMachine` disabled, 3 of 6 gate tests fail:
`concurrentClientsUnreliableNetwork` (*not linearizable on key 'k2'*), `everythingAtOnceOnRealWal` (*not
linearizable on key 'k0'*) and `retriedCommandTakesEffectOnceEndToEnd`. The reliable, partition-only and
crash-only runs still pass under the mutation, because there are almost no lost replies and therefore almost no
retries in them. That is why the lossy-network runs are part of the gate. With dedup restored, all pass.

## Running it

```bash
./mvnw -B package -DskipTests
java -jar raftkv-core/target/raftkv.jar server --id 1 --cluster 1=localhost:7001,2=localhost:7002,3=localhost:7003 --data-dir data/n1
# (same for --id 2 and --id 3 in two more terminals)
export RAFTKV_CLUSTER=1=localhost:7001,2=localhost:7002,3=localhost:7003
java -jar raftkv-core/target/raftkv.jar put city Munich
java -jar raftkv-core/target/raftkv.jar get city
java -jar raftkv-core/target/raftkv.jar status
```

## Known limits (addressed later or documented)

- The log, and the time to re-apply it after a restart, grow without bound (Phase 5: snapshots).
- Dedup sessions are never expired. Real systems expire idle sessions with a deterministic, log-driven clock.
- One outstanding command per client (by design for exactly-once); use several clients for concurrency.
- No TLS or authentication on the gRPC ports.
