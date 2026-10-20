# RaftKV — notes for Claude Code

Replicated, fault-tolerant key-value store on Raft, written from scratch in plain Java 25 (no Spring / no
framework). Portfolio project (Sem 5, Aug–Dec 2027 plan; systems depth for a Germany MS application).
Owner: Ayush (GitHub `Ayush0612005`), repo `Ayush0612005/raftkv` (public).

## Current state

- Phase 0 (skeleton): build, CI, license.
- Phase 1 (election): done, gate passed. Report `docs/phase-1.md`. Fault-injection harness in `raftkv-core/src/test/java/io/raftkv/harness`.
- Phase 2 (replication): done, gate passed. Report `docs/phase-2.md`.
- Phase 3 (persistence): done, gate passed. Report `docs/phase-3.md`. WAL = `storage/FileStorage`.
- **Next: Phase 4 (KV state machine). Do not start it until the owner says so.** Planned: `kv.proto`
  (Command {op, key, value, client_id, seq}, KvResponse), `KvStateMachine` with per-client dedup table
  (exactly-once), gRPC `Kv` service + `GrpcTransport` / Raft service for real nodes, Java client library with
  leader redirect + retry, picocli CLI, WGL-style linearizability checker in the test harness (per-key partitioning).

## Plan (one commit + `docs/phase-N.md` report per phase, pushed to `main`)

1. Election — node states, terms, RequestVote, randomized timeouts. Gate: exactly one leader; re-elects on failure.
2. Replication — AppendEntries, log matching, commit index. Gate: logs identical on all nodes.
3. Persistence — WAL + recovery. Gate: restarted node catches up.
4. KV state machine — GET/PUT/DELETE, client dedup. Gate: linearizable under concurrent clients.
5. Snapshots — compaction + InstallSnapshot. Gate: log size bounded.
6. Fault tests + bench — partition/kill tests in CI, throughput vs 3/5 nodes. Gate: results table + graphs in README.

## Hard rules

- Raft safety comes from the paper (Figure 2). Never weaken a safety check to make a test pass; a flaky test is
  a bug until proven otherwise.
- Tests must not be "fixed" by raising timeouts alone; find the cause.
- No framework dependencies in `raftkv-core` beyond gRPC, protobuf, picocli.

## Build and test

- Everything stays on drive D: project `D:\RaftKV`, Maven at `D:\tools\apache-maven-3.9.11`, local Maven repo
  `D:\RaftKV\.m2` (via `.mvn/maven.config`; run Maven from the repo root), test temp files under `target/`.
- Local: `D:\tools\apache-maven-3.9.11\bin\mvn -B verify` (CI uses `./mvnw`).
- GitHub CLI on this laptop: `D:\seismo\tools\gh\bin\gh.exe` (not on PATH).

## Working style

- Before modifying code: state the files, changes and acceptance criteria; keep changes small and staged.
- Commit messages end with a `Co-Authored-By` trailer; push to `main`.
