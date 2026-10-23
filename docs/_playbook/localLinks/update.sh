#!/bin/bash

cloneOrPullBranch () {
  if [ -d "$1" ]; then
    echo "$1 does exist - switching to and updating $3"
    # register the branch, as clones made with --single-branch only fetch their initial branch
    if ! git -C "$1" config --get-all remote.origin.fetch | grep -qF -e "refs/heads/*:" -e "refs/heads/$3:"; then
      git -C "$1" remote set-branches --add origin "$3"
    fi
    git -C "$1" fetch origin "$3" || return 1
    if git -C "$1" show-ref --verify --quiet "refs/heads/$3"; then
      git -C "$1" checkout "$3" && git -C "$1" merge --ff-only "origin/$3"
    else
      git -C "$1" checkout -b "$3" --track "origin/$3"
    fi
  else
    echo "$1 does not exist - Setting up"
    git clone --depth 30 --single-branch --branch "$3" "$2" "$1"
  fi
}

cloneOrPullBranch "AxonFramework" "https://github.com/AxonIQ/AxonFramework.git" "axon-5.1.x"
cloneOrPullBranch "extension-data-protection" "https://github.com/AxonIQ/extension-data-protection.git" "axoniq-data-protection-5.1.x"
cloneOrPullBranch "axoniq-library-ui" "https://github.com/AxonIQ/axoniq-library-ui.git" "master"

echo "You should be up-to-date now!"