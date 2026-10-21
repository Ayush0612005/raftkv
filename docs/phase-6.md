# Phase 6 — Fault tests in CI + benchmarks

**Gate:** partition/kill tests in CI; throughput vs 3/5 nodes; results table + graphs in README. **Result:
passed.** CI runs 4 jobs on every push:
- unit + gRPC tests;
- the fault-injection suite, twice in parallel;
- a chaos test that `kill -9`s and partitions a real 3-container cluster.

All four were green on the first push of this phase. The README has the results table, two SVG graphs and the
JMH numbers.

## 1. Fault tests in CI

| CI job | What | Time |
| --- | --- | --- |
| `unit + gRPC tests` | `-DexcludedGroups=fault`: log, WAL, KV, checker, real gRPC cluster | ~1 min |
| `fault injection (run 1)`, `(run 2)` | `-Dgroups=fault`: the 5 simulator suites (`@Tag("fault")`) | ~3.7 min each |
| `docker cluster chaos test` | `scripts/chaos.sh` against `docker/compose-3.yml` | ~2 min |

**Why run the fault suite twice?** The fault tests are randomized: timing, drops and crash points differ every
run. A bug that shows up 1 time in 5 is still a bug. Two independent runs per push double the chance of catching
it, at no extra wall-clock time.

**The chaos test** uses real separate JVMs in containers, real networking and real disks. It checks four things:
1. `docker kill` (SIGKILL) the leader: a new leader is elected, and committed keys are still readable;
2. restart it: it catches up (its `APPLIED` reaches the leader's `COMMIT`, read through `raftkv status`);
3. `docker network disconnect` a follower: writes continue on the majority, and it catches up after reconnecting;
4. disconnect the leader: the other two elect a new leader and accept writes; the old leader rejoins and
   converges.

Observation from the first local run: after the partitions, the term was 274. An isolated node keeps timing out
and incrementing its term (there is no Pre-Vote), so it disrupts the cluster once on rejoining. This is safe, but
it is concrete evidence for the Pre-Vote extension listed under known limits.

## 2. Benchmarks

New module `raftkv-bench` (shaded `raftkv-bench.jar`):

| Class | What |
| --- | --- |
| `WalBenchmark` (JMH) | append + sync, 1 vs 32 entries per sync, fsync on/off |
| `KvStateMachineBenchmark` (JMH) | apply PUT/GET (decode + dedup + map), snapshot serialize/restore of 100k keys |
| `LoadGenerator` | closed-loop clients over gRPC with the real client library; per-op latency; percentiles |
| `ClusterBenchmark` | starts N real server processes on localhost, warms up, sweeps 1–128 clients, writes CSV/MD/SVG |
| `Charts` | renders the SVG line charts (re-runnable from `results.csv`) |

### A benchmarking mistake caught along the way

The first smoke run gave only **236 ops/s for one client even without fsync** (p50 3.7 ms). Running the same point
twice in a row gave 472 ops/s, then **1,262 ops/s** (p50 0.68 ms). Fresh JVMs (5 servers + the load generator)
run interpreted and C1 code for their first seconds. The first points were measuring JIT warm-up, not RaftKV.
`ClusterBenchmark` now runs **15 s of unmeasured load after starting each cluster**, then 3 s warm-up + 10 s
measured per point.

### Results (fsync on, writes only, all processes on one laptop)

Hardware: AMD Ryzen 5 7235HS (8 logical CPUs), 12 GB RAM, Micron NVMe SSD, Windows 11, Java 25.

| Clients | 3 nodes ops/s | p50 | p99 | 5 nodes ops/s | p50 | p99 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 403 | 2.36 | 4.68 | 375 | 2.55 | 4.75 |
| 2 | 735 | 2.56 | 5.22 | 600 | 3.26 | 4.63 |
| 4 | 1,444 | 2.65 | 4.10 | 1,211 | 3.24 | 4.37 |
| 8 | 2,824 | 2.75 | 3.84 | 2,317 | 3.34 | 5.09 |
| 16 | 5,016 | 3.09 | 4.46 | 3,676 | 4.13 | 7.69 |
| 32 | 7,231 | 4.27 | 6.67 | 5,859 | 5.24 | 8.73 |
| 64 | 11,787 | 5.20 | 8.73 | 8,470 | 7.14 | 15.42 |
| 128 | 16,266 | 7.37 | 31.54 | 12,181 | 9.70 | 30.98 |

(latencies in ms; 0 errors in every point)

| JMH | Result |
| --- | ---: |
| WAL append+fsync, 1 per sync | 2,701 entries/s (~0.37 ms per fsync) |
| WAL append+fsync, 32 per sync | 85,818 entries/s |
| WAL append, no fsync, 1 / 32 per sync | 388K / 3.75M entries/s |
| KV apply PUT / GET | 1.9M / 2.0M ops/s |
| Snapshot serialize / restore (100k keys) | 63 ms / 33 ms |

### Reading the numbers

- **Latency floor ≈ 2 fsyncs.** One client waits for the leader's fsync, then the follower's fsync (sequential:
  the leader syncs before sending), plus two localhost round trips: about 2.4 ms. The JMH fsync cost (0.37 ms)
  accounts for well under half of it. The rest is gRPC, thread hand-offs (replicator → applier → gRPC response),
  and Windows scheduling with every process on one machine.
- **Group commit makes it scale.** From 1 to 128 clients, throughput rises about 40x while p50 latency rises about
  3x. Proposals that arrive during an in-flight round share the next AppendEntries batch and the next fsync on
  every node. The WAL micro-benchmark isolates the same effect: 32x more entries per second at 32 per sync.
- **5 vs 3 nodes:** 72–93% of the 3-node throughput (93% at 1 client, ~75% from 64 clients up), and higher
  latency under load. The leader does twice the
  replication work, and the majority needs 3 acks instead of 2, on the same 8 CPUs. On real separate machines,
  the gap would mostly come from the slowest node of the majority instead.
- **The 128-client p99 (~31 ms) is CPU saturation** of the shared laptop (up to 6 JVMs plus 128 client threads on
  8 logical CPUs), not a protocol limit. At 64 clients p99 is still 8.7 ms (3 nodes).
- **The state machine is not the bottleneck.** It applies ~2M ops/s, about 100x the cluster's throughput.
  Consensus round trips and fsync are the cost.

### What would make it faster (measured candidates, not implemented)

1. **Overlap the leader's fsync with replication** (§10.2.1 of the paper allows this, since the leader counts
   itself only once durable). This removes one fsync from the latency floor.
2. **Pipeline AppendEntries** (several in flight per follower) to keep the network busy under load.
3. **ReadIndex / lease reads**, so GETs skip the log write and the fsync entirely.

## Charts

Both charts follow a validated palette: two categorical colours checked for colour-vision-deficiency separation
(ΔE 24.7 light / 26.8 dark) and ≥ 3:1 surface contrast in light and dark mode. They have a legend plus direct
labels (with collision avoidance), 2 px lines, ≥ 8 px markers, recessive gridlines and one y-axis each. Both were
rendered and inspected: the first version had the unit label colliding with the legend, and overlapping end
labels on the latency chart, and both were fixed. The README table is the table view.

## Known limits of the benchmark

- Single machine: servers, clients and disk are shared. Absolute numbers would differ (likely higher throughput,
  and a p99 that stays flat for longer) on separate machines.
- Windows NTFS `FlushFileBuffers` semantics; Linux `fdatasync` costs differ.
- One run per configuration; the JMH runs use 1 fork and short iterations, so treat ±10–20% as noise.
- Writes only. Reads cost the same, because they also go through the log; a ReadIndex implementation would
  change that.
