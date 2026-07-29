#!/usr/bin/env bash
#
# Periodically samples system, per-process and per-container memory usage into a CSV file,
# so a CI build's memory behavior can be reconstructed and charted after the fact.
#
# Usage: memory-monitor.sh <csv-output-path> [interval-seconds]
#
# Writes a heartbeat/diagnostic trace to stderr for every stage of every iteration, so if this
# script ever stops advancing the CSV, the caller's captured stderr shows exactly which step it
# got stuck on instead of leaving a silent gap.

set -u

CSV_FILE="${1:?usage: memory-monitor.sh <csv-output-path> [interval-seconds]}"
INTERVAL="${2:-5}"

# Every docker invocation below is wrapped in `timeout --kill-after`: a container that's mid-startup
# (e.g. Testcontainers still initializing it) can make `docker exec`/`docker inspect` block on the
# daemon. Plain `timeout` only sends SIGTERM at the deadline, which a process stuck in an
# uninterruptible (D-state) wait can ignore; --kill-after follows up with SIGKILL so the loop is
# guaranteed to move on.
DOCKER_TIMEOUT=3
DOCKER_KILL_AFTER=2

log() {
  echo "[$(date -u +%H:%M:%S)] $*" >&2
}

mkdir -p "$(dirname "$CSV_FILE")"

csv_field() {
  # Wrap a value in double quotes, escaping embedded double quotes, for safe CSV output.
  printf '"%s"' "$(printf '%s' "$1" | sed 's/"/""/g')"
}

echo "timestamp,metric,name,value_mib,limit_mib,detail" > "$CSV_FILE"

log "monitor started, interval=${INTERVAL}s, docker_timeout=${DOCKER_TIMEOUT}s"

iteration=0
while true; do
  iteration=$((iteration + 1))
  ts=$(date -u +%Y-%m-%dT%H:%M:%S)
  log "iteration ${iteration}: start"

  mem_total_kb=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)
  mem_avail_kb=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)
  mem_total_mib=$(( mem_total_kb / 1024 ))
  mem_used_mib=$(( (mem_total_kb - mem_avail_kb) / 1024 ))
  echo "${ts},system,used,${mem_used_mib},${mem_total_mib}," >> "$CSV_FILE"
  log "iteration ${iteration}: system memory sampled (${mem_used_mib}/${mem_total_mib} MiB)"

  while read -r pid rss comm; do
    [ -z "${pid:-}" ] && continue
    rss_mib=$(( rss / 1024 ))
    full_args=$(tr '\0' ' ' < "/proc/${pid}/cmdline" 2>/dev/null)
    name=$(csv_field "${comm} (pid ${pid})")
    detail=$(csv_field "${full_args:0:200}")
    echo "${ts},process,${name},${rss_mib},,${detail}" >> "$CSV_FILE"
  done < <(COLUMNS=1000 ps -eo pid,rss,comm --sort=-rss --no-headers | head -12)
  log "iteration ${iteration}: process sampling done"

  log "iteration ${iteration}: checking docker availability"
  if command -v docker >/dev/null 2>&1 && timeout --kill-after="$DOCKER_KILL_AFTER" "$DOCKER_TIMEOUT" docker info >/dev/null 2>&1; then
    container_ids=$(timeout --kill-after="$DOCKER_KILL_AFTER" "$DOCKER_TIMEOUT" docker ps -q 2>/dev/null)
    log "iteration ${iteration}: docker available, containers=$(echo "$container_ids" | tr '\n' ' ')"

    for cid in $container_ids; do
      log "iteration ${iteration}: inspecting container ${cid}"
      cname=$(timeout --kill-after="$DOCKER_KILL_AFTER" "$DOCKER_TIMEOUT" docker inspect --format '{{.Name}}' "$cid" 2>/dev/null | sed 's#^/##')
      [ -z "$cname" ] && cname="$cid"

      # Testcontainers' Ryuk reaper is a minimal static-binary image with no shell; `docker exec ... sh`
      # against it fails, so skip it rather than let its OCI runtime error be misread as memory data below.
      case "$cname" in
        testcontainers-ryuk-*)
          log "iteration ${iteration}: container ${cname} (${cid}) - Ryuk reaper, skipping"
          continue
          ;;
      esac

      current=$(timeout --kill-after="$DOCKER_KILL_AFTER" "$DOCKER_TIMEOUT" docker exec "$cid" sh -c 'cat /sys/fs/cgroup/memory.current 2>/dev/null' 2>/dev/null)
      if ! [[ "$current" =~ ^[0-9]+$ ]]; then
        log "iteration ${iteration}: container ${cname} (${cid}) - memory.current not numeric (got '${current}'), skipping"
        continue
      fi
      max=$(timeout --kill-after="$DOCKER_KILL_AFTER" "$DOCKER_TIMEOUT" docker exec "$cid" sh -c 'cat /sys/fs/cgroup/memory.max 2>/dev/null' 2>/dev/null)
      if [ "$max" != "max" ] && ! [[ "$max" =~ ^[0-9]+$ ]]; then
        log "iteration ${iteration}: container ${cname} (${cid}) - memory.max not numeric (got '${max}'), treating as unlimited"
        max=""
      fi

      cur_mib=$(( current / 1024 / 1024 ))
      if [ "$max" = "max" ] || [ -z "$max" ]; then
        limit_mib=""
      else
        limit_mib=$(( max / 1024 / 1024 ))
      fi
      echo "${ts},container,$(csv_field "$cname"),${cur_mib},${limit_mib}," >> "$CSV_FILE"
      log "iteration ${iteration}: container ${cname} (${cid}) sampled (${cur_mib} MiB)"
    done
    log "iteration ${iteration}: docker check done"
  else
    log "iteration ${iteration}: docker not available or 'docker info' timed out"
  fi

  log "iteration ${iteration}: sleeping ${INTERVAL}s"
  sleep "$INTERVAL"
done
