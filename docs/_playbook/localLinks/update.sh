#!/bin/bash

cloneOrPullBranch () {
  if [ -d "$1" ]; then
    echo "$1 does exist - pulling"
      cd $1
      git pull origin $3
      git checkout -b $3
      cd ..
    else
    echo "$1 does not exist - Setting up"
      git clone --depth 30 --single-branch --branch $3 $2 $1
  fi
}

cloneOrPullBranch "AxonFramework" "https://github.com/AxonIQ/AxonFramework.git" "main"
cloneOrPullBranch "extension-data-protection" "https://github.com/AxonIQ/extension-data-protection.git" "main"
cloneOrPullBranch "axoniq-library-ui" "https://github.com/AxonIQ/axoniq-library-ui.git" "master"

echo "You should be up-to-date now!"