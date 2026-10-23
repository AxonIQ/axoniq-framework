#!/bin/bash

#
# Copyright (c) 2010-2026. Axoniq B.V.
#
# Licensed under the Axoniq Terms of Service (the "Terms"). You may not
# use this file except in compliance with the Terms, available at
# https://www.axoniq.io/legal/terms-of-service
#

# Aligns the docs build of a freshly cut release branch (e.g. axoniq-5.5.x) with its minor version:
# - the playbook builds against the matching release branches of AxonFramework and extension-data-protection
# - every docs component in this repository falls back to the minor version instead of "development",
#   so feature branches based on the release branch (e.g. backports) build the right component version
# - localLinks/update.sh checks out those same release branches for local builds with playbook-dev.yaml
#
# Usage: docs/_playbook/set-docs-version.sh <major.minor>

set -euo pipefail

if [ $# -ne 1 ] || ! [[ "$1" =~ ^[0-9]+\.[0-9]+$ ]]; then
  echo "Usage: $0 <major.minor>   (e.g. $0 5.5)" >&2
  exit 1
fi
version="$1"

playbookDir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
docsDir="$(dirname "$playbookDir")"
playbook="$playbookDir/playbook.yaml"

# Replaces the branches of the content source with the given url (the first branches key following the url line)
setSourceBranch () {
  if ! grep -q "url: $1" "$playbook"; then
    echo "No content source with url $1 found in $playbook" >&2
    exit 1
  fi
  sed -i.bak "\#url: $1#,\#branches:# s#branches:.*#branches: [ '$2' ]#" "$playbook"
  rm "$playbook.bak"
}

setSourceBranch "https://github.com/AxonIQ/AxonFramework.git" "axon-$version.x"
setSourceBranch "https://github.com/AxonIQ/extension-data-protection.git" "axoniq-data-protection-$version.x"
echo "Updated content source branches in $playbook"

# Points the local checkouts used by playbook-dev.yaml to the same release branches
updateScript="$playbookDir/localLinks/update.sh"
setLocalLinkBranch () {
  if ! grep -q "^cloneOrPullBranch \"$1\" " "$updateScript"; then
    echo "No cloneOrPullBranch call for $1 found in $updateScript" >&2
    exit 1
  fi
  sed -i.bak "s#^\(cloneOrPullBranch \"$1\" \"[^\"]*\" \)\"[^\"]*\"#\1\"$2\"#" "$updateScript"
  rm "$updateScript.bak"
}

setLocalLinkBranch "AxonFramework" "axon-$version.x"
setLocalLinkBranch "extension-data-protection" "axoniq-data-protection-$version.x"
echo "Updated local link branches in $updateScript"

for descriptor in "$docsDir"/*/antora.yml; do
  if grep -q "^  '\*':" "$descriptor"; then
    sed -i.bak "s#^  '\*':.*#  '*': '$version'#" "$descriptor"
    rm "$descriptor.bak"
    echo "Updated version fallback in $descriptor"
  fi
done

echo "Docs now build as version $version - review and commit the changes on the release branch."
