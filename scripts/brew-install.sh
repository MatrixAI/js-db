#!/usr/bin/env bash

set -o errexit   # abort on nonzero exitstatus
set -o nounset   # abort on unbound variable
set -o pipefail  # don't hide errors within pipes

export HOMEBREW_NO_INSTALL_UPGRADE=1
export HOMEBREW_NO_INSTALL_CLEANUP=1
export HOMEBREW_NO_INSTALLED_DEPENDENTS_CHECK=1
export HOMEBREW_NO_AUTO_UPDATE=1
export HOMEBREW_NO_ANALYTICS=1

brew reinstall node@20
brew link --overwrite node@20
brew install python@3.10
brew link --overwrite python@3.10

echo "/opt/homebrew/opt/python@3.10/bin" >> "$GITHUB_PATH"

echo "Python version: $(python3 --version)"
