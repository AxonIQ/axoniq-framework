#!/usr/bin/env bash
# Runs the upgrade test of the workflow index on real infrastructure: the released 5.4.1 engine leaves live
# workflows in a fresh Axon Server and PostgreSQL, then the current version takes them over with the JPA index.
#
#   upgrade.sh <graceful|crash> [tag] [extra -D flags]
#
# -Dupgrade.index=false runs the current version without the workflow index, as the control.
#
# graceful stops the 5.4.1 nodes; crash cuts their database connections first, so they cannot release their claims.
# The logs and the cases file go to results/logs/; the outcome appends to results/upgrade.csv.
set -uo pipefail

STOP=${1:-graceful}
TAG=${2:-$STOP}
shift 2 2>/dev/null || shift $#
EXTRA=("$@")
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
RESULTS="$HERE/results"
mkdir -p "$RESULTS/logs"
export JAVA_HOME="${JAVA_HOME:-$HOME/Library/Java/JavaVirtualMachines/jbr-25.0.1/Contents/Home}"
# Never pull: a missing image hangs in the credential helper. The images must already be local.
export DOCKER_CONFIG="$(mktemp -d)"
PG_PORT=55450
AS_HTTP=8025
AS_GRPC=8125
PROPS=(-Dperf.pg.url="jdbc:postgresql://localhost:$PG_PORT/perf" -Dperf.axonserver="localhost:$AS_GRPC"
       -Dupgrade.state="$RESULTS/logs/upgrade-$TAG.cases" -Dupgrade.stop="$STOP")

docker rm -f wf-campaign-pg wf-campaign-axonserver >/dev/null 2>&1 || true
docker run -d --name wf-campaign-pg -p $PG_PORT:5432 \
  -e POSTGRES_USER=perf -e POSTGRES_PASSWORD=perf -e POSTGRES_DB=perf postgres:17 -c max_connections=300 >/dev/null
until docker exec wf-campaign-pg pg_isready -U perf -d perf >/dev/null 2>&1; do sleep 1; done
docker run -d --name wf-campaign-axonserver -p $AS_HTTP:8024 -p $AS_GRPC:8124 \
  -e AXONIQ_AXONSERVER_HOSTNAME=localhost -e AXONIQ_AXONSERVER_DEVMODE_ENABLED=true \
  -e AXONIQ_AXONSERVER_PLUGINS_ENABLED=false docker.axoniq.io/axoniq/axonserver:latest >/dev/null
until curl -sf -X POST "http://localhost:$AS_HTTP/v2/cluster/init?dcb=true" >/dev/null 2>&1; do sleep 1; done
until docker logs wf-campaign-axonserver 2>&1 | grep -q "Creating DCB context: default"; do sleep 1; done
sleep 5

echo "=== phase 1 on 5.4.1 ($STOP)"
(cd "$ROOT" && ./mvnw -o test -f integrationtests/workflow-perf/upgrade-5.4.1/pom.xml "${PROPS[@]}") \
  > "$RESULTS/logs/upgrade-$TAG-5.4.1.log" 2>&1
OLD=$?
echo "    exit=$OLD"
NEW=skipped
if [ "$OLD" = 0 ]; then
  echo "=== phase 2 on the current version with the workflow index"
  (cd "$ROOT" && ./mvnw -o test -Pperf -pl integrationtests/workflow-perf -am \
      -Dtest=WorkflowIndexUpgradeTest -Dsurefire.failIfNoSpecifiedTests=false "${PROPS[@]}" \
      ${EXTRA[@]+"${EXTRA[@]}"}) \
    > "$RESULTS/logs/upgrade-$TAG-current.log" 2>&1
  NEW=$?
  echo "    exit=$NEW"
fi
echo "$TAG,$STOP,$OLD,$NEW" >> "$RESULTS/upgrade.csv"
docker exec wf-campaign-pg psql -U perf -d perf -c \
  "select version, effect, count(*) from upgrade_effect group by version, effect order by version, effect" \
  > "$RESULTS/logs/upgrade-$TAG.effects.txt" 2>&1 || true
docker rm -f wf-campaign-pg wf-campaign-axonserver >/dev/null 2>&1 || true
