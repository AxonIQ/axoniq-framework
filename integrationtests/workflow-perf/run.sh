#!/usr/bin/env bash
# Runs the hibernation load test: fresh containers and one JVM per size. PostgreSQL always runs, as it holds the JPA
# token store and the JPA workflow index; with the axonserver store, Axon Server holds the events.
#
#   run.sh <postgres|axonserver> <hibernation true|false> <snapshots true|false> <sizes, comma separated> [extra -D flags]
#
# Example: run.sh axonserver true true 100,1000,10000 -Dperf.index=jpa
# TEST=<method> picks the test method, hibernatedWorkflowsAtScale by default.
# Results append to results/results.csv, or results/results-<tag>.csv with PERF_TAG=<tag>; full logs go to results/logs/.
set -euo pipefail

STORE=$1
HIBERNATION=$2
SNAPSHOTS=$3
SIZES=$4
shift 4
EXTRA=("$@")

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
RESULTS="$HERE/results"
mkdir -p "$RESULTS/logs"
TAG="${PERF_TAG:-}"
OUT="$RESULTS/results${TAG:+-$TAG}.csv"
export JAVA_HOME="${JAVA_HOME:-$HOME/Library/Java/JavaVirtualMachines/jbr-25.0.1/Contents/Home}"
# Never pull: a missing image hangs in the credential helper. The images must already be local.
export DOCKER_CONFIG="$(mktemp -d)"
PG_IMAGE=postgres:17
AS_IMAGE=docker.axoniq.io/axoniq/axonserver:latest

start_postgres() {
  docker rm -f hib-perf-pg >/dev/null 2>&1 || true
  docker run -d --name hib-perf-pg -p 55440:5432 \
    -e POSTGRES_USER=perf -e POSTGRES_PASSWORD=perf -e POSTGRES_DB=perf \
    "$PG_IMAGE" -c max_connections=300 -c shared_buffers=1GB >/dev/null
  until docker exec hib-perf-pg pg_isready -U perf -d perf >/dev/null 2>&1; do sleep 1; done
  sleep 2
}

start_axonserver() {
  docker rm -f hib-perf-axonserver >/dev/null 2>&1 || true
  docker run -d --name hib-perf-axonserver -p 8024:8024 -p 8124:8124 \
    -e AXONIQ_AXONSERVER_HOSTNAME=localhost -e AXONIQ_AXONSERVER_DEVMODE_ENABLED=true \
    -e AXONIQ_AXONSERVER_PLUGINS_ENABLED=false \
    "$AS_IMAGE" >/dev/null
  until curl -sf -X POST "http://localhost:8024/v2/cluster/init?dcb=true" >/dev/null 2>&1; do sleep 1; done
  # The context is listed before its DCB event store has a leader; wait for the store itself.
  until docker logs hib-perf-axonserver 2>&1 | grep -q "Creating DCB context: default"; do sleep 1; done
  sleep 5
}

IFS=',' read -ra SIZE_LIST <<< "$SIZES"
for SIZE in "${SIZE_LIST[@]}"; do
  INDEX_ARM="$([[ " ${EXTRA[*]-} " == *" -Dperf.index=jpa "* ]] && echo +jpaindex || echo +jpatokens)"
  ARM="$([ "$HIBERNATION" = true ] && echo hibernation || echo resident)$([ "$SNAPSHOTS" = true ] && echo +snap || echo -snap)$INDEX_ARM"
  LOG="$RESULTS/logs/$STORE-$ARM-$SIZE${TAG:+-$TAG}.log"
  echo "=== $STORE $ARM $SIZE -> $LOG"
  start_postgres
  if [ "$STORE" = axonserver ]; then start_axonserver; fi
  set +e
  (cd "$ROOT" && ./mvnw -o test -Pperf -pl integrationtests/workflow-perf \
      -Dtest="HibernationLoadTest#${TEST:-hibernatedWorkflowsAtScale}" -Dsurefire.failIfNoSpecifiedTests=false \
      -Dperf.store="$STORE" -Dperf.size="$SIZE" -Dperf.hibernation="$HIBERNATION" -Dperf.snapshots="$SNAPSHOTS" \
      -Dperf.out="$OUT" ${EXTRA[@]+"${EXTRA[@]}"}) > "$LOG" 2>&1
  STATUS=$?
  set -e
  WARNS=$(grep -c " WARN " "$LOG" || true)
  ERRORS=$(grep " ERROR " "$LOG" | grep -c -v "Waiting for license" || true)
  echo "$STORE,$ARM,$SIZE,log_warn_lines,$WARNS" >> "$OUT"
  echo "$STORE,$ARM,$SIZE,log_error_lines,$ERRORS" >> "$OUT"
  echo "$STORE,$ARM,$SIZE,exit_status,$STATUS" >> "$OUT"
  echo "    exit=$STATUS warn=$WARNS error=$ERRORS"
  if [ "$STATUS" != 0 ]; then
    echo "    stopped at the first failing size"
    break
  fi
done
