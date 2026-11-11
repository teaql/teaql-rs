#!/usr/bin/env bash
set -euo pipefail
(( $# == 0 )) || { printf 'Usage: bash benchmark-mainstream.sh\n'; exit 2; }
exec node "$(dirname "$0")/verify-mainstream-reference.cjs" --release
