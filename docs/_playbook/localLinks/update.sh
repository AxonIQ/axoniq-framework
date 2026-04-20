#!/bin/bash

cloneOrPullMaster () {
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

# Axon Framework has two branches: master and 4.10.x
cloneOrPullMaster "AxonFramework" "https://github.com/AxonIQ/AxonFramework.git" "main"
cloneOrPullMaster "axoniq-library-ui" "https://github.com/AxonIQ/axoniq-library-ui.git" "master"

echo "You should be up-to-date now!"