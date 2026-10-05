#!/usr/bin/env bash
#
# The guard's verdict fold is the one piece of it that can be wrong quietly: a
# fold that returns "green" where the contract says "red" makes the guard stand
# still on exactly the commit it exists to undo. The fold is exercised against
# the cases the Rust contract pins, without a network or a repository.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

bash "${here}/../revert-guard.sh" --self-test
