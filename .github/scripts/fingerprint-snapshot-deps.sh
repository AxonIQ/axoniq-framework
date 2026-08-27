#!/usr/bin/env bash
# Detects external SNAPSHOT dependencies actually resolved for this reactor build (e.g. Axon
# Framework, when axon-framework.version points at a SNAPSHOT), hashes their jar content, and
# drops the result into every actually-active reactor module's src/main/resources. The Maven
# Build Cache Extension already hashes that directory by default, so a changed SNAPSHOT jar now
# invalidates the cache the same way a real source change would -- closing the gap where a
# SNAPSHOT's version string stays the same across redeploys while its content doesn't.
set -euo pipefail

REACTOR_GROUP_ID="io.axoniq.framework"
FINGERPRINT_FILE="axon-snapshot.fingerprint"
LIST_FILE=".snapshot-deps.txt"

echo "Resolving reactor dependencies (forcing a fresh SNAPSHOT check)..."
./mvnw -B -U -ntp -q dependency:list -DoutputAbsoluteArtifactFilename=true -Dsort=true -DoutputFile="$LIST_FILE"

FINGERPRINT_TMP=$(mktemp)
MODULE_DIRS_TMP=$(mktemp)
trap 'rm -f "$FINGERPRINT_TMP" "$MODULE_DIRS_TMP"' EXIT

# Only modules Maven itself actually built get a $LIST_FILE -- this is the authoritative
# reactor module list, correctly respecting profile activation and excluding unrelated trees
# like _multitenancy_poc that happen to contain their own pom.xml files.
find . -name "$LIST_FILE" -exec dirname {} \; > "$MODULE_DIRS_TMP"

find . -name "$LIST_FILE" -exec cat {} + \
  | sed -E 's/ -- module.*$//' \
  | grep -E ':[0-9][^:]*-SNAPSHOT:(compile|test|runtime|provided|system):' \
  | grep -v -E "^[[:space:]]*${REACTOR_GROUP_ID//./\.}:" \
  | sed -E 's/^.*:(compile|test|runtime|provided|system):(.*)$/\2/' \
  | sed -E 's/ \(optional\)$//' \
  | sort -u \
  | while IFS= read -r artifact_path; do
      dir=$(dirname "$artifact_path")
      base=$(basename "$artifact_path")
      hash=$(cd "$dir" && sha256sum -- "$base" | cut -d' ' -f1)
      echo "$base $hash"
    done | sort > "$FINGERPRINT_TMP"

find . -name "$LIST_FILE" -delete

if [ -s "$FINGERPRINT_TMP" ]; then
  count=$(wc -l < "$FINGERPRINT_TMP")
  echo "Found $count external SNAPSHOT dependency artifact(s); fingerprinting $(wc -l < "$MODULE_DIRS_TMP") reactor module(s)."
  while IFS= read -r module_dir; do
    mkdir -p "$module_dir/src/main/resources"
    cp "$FINGERPRINT_TMP" "$module_dir/src/main/resources/$FINGERPRINT_FILE"
  done < "$MODULE_DIRS_TMP"
else
  echo "No external SNAPSHOT dependencies resolved; nothing to fingerprint."
fi
