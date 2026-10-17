#!/usr/bin/env bash
set -euo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
: "${TEAQL_S3_ENDPOINT:?dedicated local test endpoint required}"
: "${TEAQL_S3_BUCKET:?dedicated test bucket required}"
: "${TEAQL_S3_ACCESS_KEY:?test access key required}"
: "${TEAQL_S3_SECRET_KEY:?test secret key required}"
cd "$repo"
cargo test -p teaql-core -p teaql-runtime -p teaql-object-storage-s3 object_storage
cargo run -p teaql-object-storage-s3 --example object_storage
