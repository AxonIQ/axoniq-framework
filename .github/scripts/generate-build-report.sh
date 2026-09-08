#!/usr/bin/env bash
# Parses a captured Maven build log into a per-module markdown status table: cache hit/miss,
# success/failure/skipped, build time, and (for a failed module) the first failing test and a
# short error message.
#
# The Reactor Summary is the only part of the log that always lists every module (including ones
# -fae skipped entirely because a dependency failed), so it drives the row list. Cache hit/miss is
# only ever emitted for a module that was actually attempted, and is keyed there by Maven
# coordinate (from the "Attempting to restore project" line), not by the human-readable name the
# summary uses. Failure detail comes from the surefire/failsafe XML reports rather than scraping
# console text -- surefire's <failure>/<error> "message" attribute is a stable, structured field,
# unlike the free-form console rendering of the same failure. A name-to-artifactId map and an
# artifactId-to-directory map, both built from the working tree's own pom.xml files (skipping XML
# comments, which can contain tag-like text), tie the log's two different keying schemes and the
# module's own directory together.
set -euo pipefail

LOG_FILE="$1"
LABEL="$2"
OUT_FILE="$3"

PLAIN_LOG=$(mktemp)
NAME_MAP=$(mktemp)
DIR_MAP=$(mktemp)
RAW_ROWS=$(mktemp)
trap 'rm -f "$PLAIN_LOG" "$NAME_MAP" "$DIR_MAP" "$RAW_ROWS"' EXIT
sed -E 's/\x1b\[[0-9;]*m//g' "$LOG_FILE" > "$PLAIN_LOG"

: > "$NAME_MAP"
: > "$DIR_MAP"
find . -name pom.xml -not -path '*/target/*' -not -path '*/_multitenancy_poc/*' -not -path '*/_archive/*' | while IFS= read -r pom; do
  moduleDir=$(dirname "$pom")
  perl -0777 -pe 's/<!--.*?-->//gs' "$pom" | awk -v dir="$moduleDir" -v namemap="$NAME_MAP" -v dirmap="$DIR_MAP" '
    /<parent>/ { inParent = 1 }
    /<\/parent>/ { inParent = 0; next }
    inParent { next }
    /<dependencies>|<build>|<profiles>/ { exit }
    /<artifactId>/ && artifactId == "" { line = $0; gsub(/.*<artifactId>|<\/artifactId>.*/, "", line); artifactId = line }
    /<name>/ && name == "" { line = $0; gsub(/.*<name>|<\/name>.*/, "", line); name = line }
    END {
        disp = (name != "" ? name : artifactId)
        print disp "\t" artifactId >> namemap
        print artifactId "\t" dir >> dirmap
    }
  '
done
sort -u -o "$NAME_MAP" "$NAME_MAP"
sort -u -o "$DIR_MAP" "$DIR_MAP"

# Emit raw tab-separated rows: artifact, cache, status, time (no failure detail yet).
awk -F'\t' '
NR == FNR { nameToArtifact[$1] = $2; next }

/^\[INFO\] Attempting to restore project / {
    line = $0
    sub(/^\[INFO\] Attempting to restore project /, "", line)
    sub(/ from build cache$/, "", line)
    split(line, parts, ":")
    currentArtifact = parts[2]
    next
}
/^\[INFO\] Found cached build, restoring / { cacheStatus[currentArtifact] = "hit"; next }
/^\[INFO\] Local build was not found by checksum / { cacheStatus[currentArtifact] = "miss"; next }

/^\[INFO\] Reactor Summary/ { inSummary = 1; summaryCount = 0; next }
inSummary && /^\[INFO\] (BUILD SUCCESS|BUILD FAILURE)/ { inSummary = 0; next }
inSummary && /^\[INFO\] [A-Za-z0-9].* \.+ (SUCCESS|FAILURE|SKIPPED)/ {
    summaryCount++
    line = $0
    sub(/^\[INFO\] /, "", line)
    rowTime = ""
    if (match(line, / \[[0-9:. ]+ (ms|s|min|h)\]$/)) {
        t = substr(line, RSTART, RLENGTH)
        gsub(/^ \[ */, "", t); gsub(/\]$/, "", t)
        rowTime = t
        line = substr(line, 1, RSTART - 1)
    }
    rowStatus = ""
    if (match(line, /(SUCCESS|FAILURE|SKIPPED)$/)) {
        rowStatus = substr(line, RSTART, RLENGTH)
        line = substr(line, 1, RSTART - 1)
    }
    gsub(/[. ]+$/, "", line)
    summaryName[summaryCount] = line
    summaryStatus[summaryCount] = rowStatus
    summaryTime[summaryCount] = rowTime
    next
}

END {
    for (i = 1; i <= summaryCount; i++) {
        name = summaryName[i]
        artifact = (name in nameToArtifact) ? nameToArtifact[name] : name
        cache = (cacheStatus[artifact] == "hit") ? "cached" : (cacheStatus[artifact] == "miss") ? "built" : "-"
        time = (summaryTime[i] != "") ? summaryTime[i] : "-"
        printf "%s\t%s\t%s\t%s\n", artifact, cache, summaryStatus[i], time
    }
}
' "$NAME_MAP" "$PLAIN_LOG" > "$RAW_ROWS"

# Given a module directory, print "testId<TAB>message" for the first <failure>/<error> found
# across its surefire/failsafe XML reports, or nothing if none is found.
first_failure() {
  local dir="$1"
  local xml result
  for xml in "$dir"/target/surefire-reports/TEST-*.xml "$dir"/target/failsafe-reports/TEST-*.xml; do
    [ -f "$xml" ] || continue
    result=$(awk '
      /<testcase / {
          line = $0
          tn = ""; tc = ""
          if (match(line, /name="[^"]*"/)) tn = substr(line, RSTART + 6, RLENGTH - 7)
          if (match(line, /classname="[^"]*"/)) tc = substr(line, RSTART + 11, RLENGTH - 12)
      }
      /<(failure|error)[ >]/ {
          line = $0
          msg = ""
          if (match(line, /message="[^"]*"/)) msg = substr(line, RSTART + 9, RLENGTH - 10)
          print tc "." tn "\t" msg
          exit
      }
    ' "$xml")
    if [ -n "$result" ]; then
      printf '%s\n' "$result"
      return
    fi
  done
}

unescape_xml() {
  sed -e 's/&#13;//g' -e 's/&#10;/; /g' -e 's/&quot;/"/g' -e "s/&apos;/'/g" \
      -e 's/&lt;/</g' -e 's/&gt;/>/g' -e 's/&amp;/\&/g' \
    | tr '\n' ' ' \
    | sed -E 's/[[:space:]]+/ /g; s/^[[:space:];]+//; s/[[:space:];]+$//; s/;( ;)+/;/g'
}

total=$(wc -l < "$RAW_ROWS")
cachedCount=$(awk -F'\t' '$2 == "cached"' "$RAW_ROWS" | wc -l)
builtCount=$(awk -F'\t' '$2 == "built"' "$RAW_ROWS" | wc -l)
successCount=$(awk -F'\t' '$3 == "SUCCESS"' "$RAW_ROWS" | wc -l)
failureCount=$(awk -F'\t' '$3 == "FAILURE"' "$RAW_ROWS" | wc -l)
skippedCount=$(awk -F'\t' '$3 == "SKIPPED"' "$RAW_ROWS" | wc -l)

if [ "$total" -eq 0 ]; then
  summary="#### ${LABEL} - **UNKNOWN** - no per-module data found; the build likely did not complete (e.g. timed out or was cancelled) -- check the build step's own log"
elif [ "$failureCount" -gt 0 ]; then
  summary="#### ${LABEL} - **FAILURE** - ${successCount} succeeded, ${failureCount} failed, ${skippedCount} skipped (${cachedCount} cached, ${builtCount} built)"
else
  summary="#### ${LABEL} - **SUCCESS** - ${total} modules (${cachedCount} cached, ${builtCount} built)"
fi

{
  echo "$summary"
  echo
  echo "<details>"
  echo "<summary>Per-module details</summary>"
  echo
  echo "| Module | Cache | Status | Time | Details |"
  echo "|---|---|---|---|---|"
  while IFS=$'\t' read -r artifact cache status time; do
    details="-"
    if [ "$status" = "FAILURE" ]; then
      moduleDir=$(awk -F'\t' -v a="$artifact" '$1 == a { print $2; exit }' "$DIR_MAP")
      if [ -n "$moduleDir" ]; then
        failureLine=$(first_failure "$moduleDir")
        if [ -n "$failureLine" ]; then
          testId=$(printf '%s' "$failureLine" | cut -f1)
          rawMsg=$(printf '%s' "$failureLine" | cut -f2-)
          shortMsg=$(printf '%s' "$rawMsg" | unescape_xml)
          if [ ${#shortMsg} -gt 120 ]; then
            shortMsg="${shortMsg:0:117}..."
          fi
          testId=${testId//|/\|}
          shortMsg=${shortMsg//|/\|}
          details="\`${testId}\` - ${shortMsg}"
        fi
      fi
    fi
    printf '| %s | %s | %s | %s | %s |\n' "$artifact" "$cache" "$status" "$time" "$details"
  done < "$RAW_ROWS"
  echo
  echo "</details>"
} > "$OUT_FILE"

echo "Report written to $OUT_FILE"
