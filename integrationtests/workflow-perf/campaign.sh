#!/usr/bin/env bash
# Runs the fault campaign of the workflow index on real infrastructure: a fresh PostgreSQL and a fresh Axon Server
# for every run, one JVM per arm and seed.
#
#   campaign.sh <arms, comma separated> <seeds, comma separated> [extra -D flags]
#
# Example: campaign.sh noindex,indexed 1,2,3 -Dcampaign.ops=40
# STORE=postgres keeps the events in PostgreSQL instead of Axon Server. TAG=<name> prefixes the log names.
# Results append to results/campaign.csv; the log and the operation trace of each run go to results/logs/.
set -uo pipefail

ARMS=$1
SEEDS=$2
shift 2
EXTRA=("$@")

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
RESULTS="$HERE/results"
mkdir -p "$RESULTS/logs"
OUT="$RESULTS/campaign.csv"
export JAVA_HOME="${JAVA_HOME:-$HOME/Library/Java/JavaVirtualMachines/jbr-25.0.1/Contents/Home}"
# Never pull: a missing image hangs in the credential helper. The images must already be local.
export DOCKER_CONFIG="$(mktemp -d)"
PG_IMAGE=postgres:17
AS_IMAGE=docker.axoniq.io/axoniq/axonserver:latest
# Ports and container names of its own, so a load test can keep its containers. PG_PORT, AS_HTTP, AS_GRPC
# and CONTAINER_PREFIX override them, so two runs can share the machine.
PG_PORT=${PG_PORT:-55450}
AS_HTTP=${AS_HTTP:-8025}
AS_GRPC=${AS_GRPC:-8125}

start_postgres() {
  docker rm -f ${CONTAINER_PREFIX:-wf-campaign}-pg >/dev/null 2>&1 || true
  docker run -d --name ${CONTAINER_PREFIX:-wf-campaign}-pg -p $PG_PORT:5432 \
    -e POSTGRES_USER=perf -e POSTGRES_PASSWORD=perf -e POSTGRES_DB=perf \
    "$PG_IMAGE" -c max_connections=300 >/dev/null
  until docker exec ${CONTAINER_PREFIX:-wf-campaign}-pg pg_isready -U perf -d perf >/dev/null 2>&1; do sleep 1; done
  sleep 2
}

start_axonserver() {
  docker rm -f ${CONTAINER_PREFIX:-wf-campaign}-axonserver >/dev/null 2>&1 || true
  docker run -d --name ${CONTAINER_PREFIX:-wf-campaign}-axonserver -p $AS_HTTP:8024 -p $AS_GRPC:8124 \
    -e AXONIQ_AXONSERVER_HOSTNAME=localhost -e AXONIQ_AXONSERVER_DEVMODE_ENABLED=true \
    -e AXONIQ_AXONSERVER_PLUGINS_ENABLED=false \
    "$AS_IMAGE" >/dev/null
  until curl -sf -X POST "http://localhost:$AS_HTTP/v2/cluster/init?dcb=true" >/dev/null 2>&1; do sleep 1; done
  # The context is listed before its DCB event store has a leader; wait for the store itself.
  until docker logs ${CONTAINER_PREFIX:-wf-campaign}-axonserver 2>&1 | grep -q "Creating DCB context: default"; do sleep 1; done
  sleep 5
}

IFS=',' read -ra ARM_LIST <<< "$ARMS"
IFS=',' read -ra SEED_LIST <<< "$SEEDS"
for ARM in "${ARM_LIST[@]}"; do
  for SEED in "${SEED_LIST[@]}"; do
    LOG="$RESULTS/logs/campaign-${TAG:+$TAG-}$ARM-$SEED.log"
    TRACE="$RESULTS/logs/campaign-${TAG:+$TAG-}$ARM-$SEED.trace"
    echo "=== $ARM seed $SEED -> $LOG"
    start_postgres
    if [ "${STORE:-axonserver}" = axonserver ]; then start_axonserver; fi
    (cd "$ROOT" && ./mvnw -o test -Pperf -pl integrationtests/workflow-perf -am \
        -Dtest=WorkflowIndexFaultCampaignTest -Dsurefire.failIfNoSpecifiedTests=false \
        -Dperf.pg.url="jdbc:postgresql://localhost:$PG_PORT/perf" -Dperf.axonserver="localhost:$AS_GRPC" \
        -Dcampaign.arm="$ARM" -Dcampaign.seed="$SEED" -Dcampaign.trace="$TRACE" \
        -Dcampaign.indexLogLevel=DEBUG -Dcampaign.store="${STORE:-axonserver}" \
        ${EXTRA[@]+"${EXTRA[@]}"}) > "$LOG" 2>&1
    STATUS=$?
    if [ "$STATUS" != 0 ]; then
      # What the stores held when the run failed.
      docker exec ${CONTAINER_PREFIX:-wf-campaign}-pg psql -U perf -d perf -c \
        "select workflowid, segmentkey, wakesat from workflowindexentry order by workflowid" -c \
        "select workflowid, stepname from workflowindexwaitentry order by workflowid" -c \
        "select processorname, segment, mask, owner from tokenentry order by segment" \
        > "${LOG%.log}.db.txt" 2>&1 || true
    fi
    WARNS=$(grep -c " WARN " "$LOG" || true)
    CLAIMS=$(grep -c "from the index:" "$LOG" || true)
    echo "${TAG:-base},${STORE:-axonserver},$ARM,$SEED,$STATUS,$WARNS,$CLAIMS" >> "$OUT"
    echo "    exit=$STATUS warn=$WARNS claims_from_index=$CLAIMS"
  done
done
docker rm -f ${CONTAINER_PREFIX:-wf-campaign}-pg ${CONTAINER_PREFIX:-wf-campaign}-axonserver >/dev/null 2>&1 || true
