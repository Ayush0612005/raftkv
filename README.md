# RaftKV

A replicated, fault-tolerant key-value store built from scratch on the
[Raft consensus algorithm](https://raft.github.io/raft.pdf), in plain Java 25 with no framework. RaftKV covers
the same ground as the internals of etcd, Kafka's KRaft and replicated databases: leader election, log
replication, a write-ahead log, crash recovery, exactly-once client semantics and log compaction.

Status: **Phase 1 complete** (leader election). See [Build phases](#build-phases).

## Tech stack

| Area | Choice |
| --- | --- |
| Language | Java 25, plain Java (no framework), virtual threads for RPC fan-out |
| Networking | gRPC-java + Protocol Buffers (node-to-node RPC and client API) |
| Storage | Own write-ahead log (append-only, CRC-checked, fsync) + periodic snapshots |
| Client | Java client library + picocli CLI |
| Testing | JUnit 5, own fault-injection harness (kill leader, drop/delay messages, partitions), linearizability checker |
| Benchmarks | JMH (micro) + load generator (throughput/latency vs cluster size) |
| Run | Docker Compose (3-node and 5-node clusters), GitHub Actions |

## Build phases

| Phase | What | Done when |
| --- | --- | --- |
| 1. Election | Node states, terms, RequestVote, randomized timeouts | Exactly one leader; re-elects on failure |
| 2. Replication | AppendEntries, log matching, commit index | Logs identical on all nodes |
| 3. Persistence | WAL + recovery after crash | Restarted node catches up |
| 4. KV state machine | GET/PUT/DELETE, client dedup (exactly-once) | Linearizable under concurrent clients |
| 5. Snapshots | Log compaction + InstallSnapshot | Log size bounded |
| 6. Fault tests + bench | Partition/kill tests in CI, throughput vs 3/5 nodes | Results table + graphs in README |

Each phase has a report in [`docs/`](docs).

## Build

Requires JDK 25. The Maven wrapper downloads Maven itself.

```bash
./mvnw -B verify
```

## References

- D. Ongaro and J. Ousterhout, *In Search of an Understandable Consensus Algorithm (Extended Version)*, 2014.
- MIT 6.5840 (Distributed Systems) lab structure, re-implemented in Java.

## License

Apache-2.0
