#!/bin/bash

#
# Copyright (c) 2010-2026. Axoniq B.V.
#
# Licensed under the Axoniq Terms of Service (the "Terms"). You may not
# use this file except in compliance with the Terms, available at
# https://www.axoniq.io/legal/terms-of-service
#

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
cloneOrPullBranch "extension-workflow" "https://github.com/AxonIQ/extension-workflow.git" "main"
cloneOrPullBranch "axoniq-library-ui" "https://github.com/AxonIQ/axoniq-library-ui.git" "master"

echo "You should be up-to-date now!"