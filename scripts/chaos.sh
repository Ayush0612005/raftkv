#!/usr/bin/env bash
# Fault test against a real 3-node cluster in Docker: separate JVMs, real network, real disks.
#   1. kill -9 the leader (docker kill): a new leader takes over, no committed write is lost
#   2. restart it: it catches up from its WAL and the new leader
#   3. partition a follower off the network: the majority keeps serving writes; it catches up after healing
#   4. partition the leader: the other two elect a new leader; the old one steps down after healing
# Needs: docker (with compose v2), java 25, and the jar built (./mvnw -B package -DskipTests).
set -euo pipefail
cd "$(dirname "$0")/.."

PROJECT=raftkv3
COMPOSE="docker compose -f docker/compose-3.yml"
NETWORK="${PROJECT}_default"
JAR=raftkv-core/target/raftkv.jar
export RAFTKV_CLUSTER=1=localhost:7001,2=localhost:7002,3=localhost:7003

kv() { java --sun-misc-unsafe-memory-access=allow -jar "$JAR" "$@"; }
container() { echo "${PROJECT}-node$1-1"; }
status() { kv status 2>/dev/null; }
# status columns: ID ADDRESS ROLE TERM LEADER LAST_LOG COMMIT APPLIED SNAPSHOT LOG_SIZE
leader() { status | awk '$3 == "LEADER" { print $1 }' | head -1; }
applied_of() { status | awk -v id="$1" '$1 == id { print $8 }'; }
commit_of() { status | awk -v id="$1" '$1 == id { print $7 }'; }

wait_leader() {  # wait_leader [id-to-avoid]
  for _ in $(seq 1 60); do
    local l
    l=$(leader || true)
    if [ -n "$l" ] && [ "$l" != "${1:-}" ]; then echo "$l"; return 0; fi
    sleep 1
  done
  echo "FAIL: no leader elected" >&2
  return 1
}

expect() {  # expect key value
  local got
  got=$(kv get "$1")
  if [ "$got" != "$2" ]; then echo "FAIL: $1 = '$got', expected '$2'" >&2; exit 1; fi
}

wait_caught_up() {  # wait_caught_up node-id
  for _ in $(seq 1 60); do
    local l target applied
    l=$(leader || true)
    if [ -n "$l" ]; then
      target=$(commit_of "$l"); applied=$(applied_of "$1")
      if [ -n "$applied" ] && [ -n "$target" ] && [ "$applied" -ge "$target" ]; then
        echo "node$1 caught up (applied $applied >= leader commit $target)"; return 0
      fi
    fi
    sleep 1
  done
  echo "FAIL: node$1 did not catch up" >&2
  status >&2
  return 1
}

docker build -q -t raftkv:latest . >/dev/null
$COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true
$COMPOSE up -d
trap '$COMPOSE logs --no-color > chaos-logs.txt 2>&1 || true; $COMPOSE down -v >/dev/null 2>&1 || true' EXIT

L=$(wait_leader)
echo "leader: node$L"
for i in $(seq 1 20); do kv put "k$i" "v$i" >/dev/null; done
expect k20 v20

echo "== 1. kill -9 the leader (node$L)"
docker kill "$(container "$L")" >/dev/null
NEW=$(wait_leader "$L")
echo "new leader: node$NEW"
kv put after-kill yes >/dev/null
expect k1 v1
expect after-kill yes

echo "== 2. restart node$L"
docker start "$(container "$L")" >/dev/null
for i in $(seq 21 40); do kv put "k$i" "v$i" >/dev/null; done
wait_caught_up "$L"

F=$(( $(leader) % 3 + 1 ))
echo "== 3. partition follower node$F"
docker network disconnect "$NETWORK" "$(container "$F")"
for i in $(seq 41 60); do kv put "k$i" "v$i" >/dev/null; done
expect k60 v60
docker network connect --alias "node$F" "$NETWORK" "$(container "$F")"
wait_caught_up "$F"

L=$(leader)
echo "== 4. partition the leader (node$L)"
docker network disconnect "$NETWORK" "$(container "$L")"
NEW=$(wait_leader "$L")
echo "new leader: node$NEW"
kv put during-partition yes >/dev/null
docker network connect --alias "node$L" "$NETWORK" "$(container "$L")"
wait_caught_up "$L"
expect during-partition yes
expect k40 v40

echo
status
echo "CHAOS TEST PASSED"
