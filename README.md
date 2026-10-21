# RaftKV

A replicated, fault-tolerant key-value store built from scratch on the
[Raft consensus algorithm](https://raft.github.io/raft.pdf), in plain Java 25 with no framework. RaftKV covers
the same ground as the internals of etcd, Kafka's KRaft and replicated databases: leader election, log
replication, a write-ahead log, crash recovery, exactly-once client semantics and log compaction.

Status: **Phase 5 complete** (snapshots: bounded log, InstallSnapshot). See [Quick start](#quick-start) and [Build phases](#build-phases).

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
./mvnw -B verify          # build + all tests (unit, fault-injection, linearizability, gRPC)
```

## Quick start

```bash
./mvnw -B package -DskipTests
CLUSTER=1=localhost:7001,2=localhost:7002,3=localhost:7003
java -jar raftkv-core/target/raftkv.jar server --id 1 --cluster $CLUSTER --data-dir data/n1 &
java -jar raftkv-core/target/raftkv.jar server --id 2 --cluster $CLUSTER --data-dir data/n2 &
java -jar raftkv-core/target/raftkv.jar server --id 3 --cluster $CLUSTER --data-dir data/n3 &

export RAFTKV_CLUSTER=$CLUSTER
java -jar raftkv-core/target/raftkv.jar put city Munich
java -jar raftkv-core/target/raftkv.jar get city
java -jar raftkv-core/target/raftkv.jar status
```

Java client:

```java
try (RaftKvClient kv = RaftKvClient.connect(Map.of(1, "localhost:7001", 2, "localhost:7002", 3, "localhost:7003"))) {
    kv.put("city", "Munich");
    Optional<String> city = kv.get("city");
}
```

## References

- D. Ongaro and J. Ousterhout, *In Search of an Understandable Consensus Algorithm (Extended Version)*, 2014.
- MIT 6.5840 (Distributed Systems) lab structure, re-implemented in Java.

## License

Apache-2.0
