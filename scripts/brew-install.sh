#!/usr/bin/env bash

set -o errexit   # abort on nonzero exitstatus
set -o nounset   # abort on unbound variable
set -o pipefail  # don't hide errors within pipes

export HOMEBREW_NO_INSTALL_UPGRADE='true'
export HOMEBREW_NO_INSTALL_CLEANUP='true'
export HOMEBREW_NO_INSTALLED_DEPENDENTS_CHECK='true'
export HOMEBREW_NO_AUTO_UPDATE='true'

brew install node@16
brew link --overwrite node@16
brew install python@3.9
brew link --overwrite python@3.9
