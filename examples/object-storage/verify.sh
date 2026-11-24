#!/usr/bin/env bash
set -euo pipefail
example="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$example/../.." && pwd)"
: "${TEAQL_S3_ENDPOINT:?dedicated local test endpoint required}"
: "${TEAQL_S3_BUCKET:?dedicated test bucket required}"
: "${TEAQL_S3_ACCESS_KEY:?test access key required}"
: "${TEAQL_S3_SECRET_KEY:?test secret key required}"
if [[ -n "${TEAQL_ORDER_ATTACHMENT_EVIDENCE_DIR:-}" ]]; then
  evidence="$TEAQL_ORDER_ATTACHMENT_EVIDENCE_DIR"
  mkdir -p "$(dirname "$evidence")"
  mkdir "$evidence"
else
  evidence="$(mktemp -d -t teaql-rust-order-attachment.XXXXXXXX)"
fi
evidence="$(cd "$evidence" && pwd)"
app="$example/rust-app-console"
fingerprint() {
  (cd "$example" && find rust-lib-core rust-app-console -type f -not -path '*/target/*' -print0 | sort -z | xargs -0 sha256sum)
}
fingerprint > "$evidence/source-before.sha256"
cd "$repo"
if [[ "${1:-}" != --workspace-only ]]; then
  cargo test --locked -p teaql-core -p teaql-runtime -p teaql-object-storage-s3 object_storage 2>&1 | tee "$evidence/provider-tests.log"
  cargo run --locked -p teaql-object-storage-s3 --example object_storage | tee "$evidence/s3-calibration.log"
fi
cd "$evidence"
cargo metadata --locked --manifest-path "$app/Cargo.toml" --format-version 1 |
  python3 -B -c 'import json,sys; m=json.load(sys.stdin); print(json.dumps({p["name"]:p["manifest_path"] for p in m["packages"] if p["source"] is None},sort_keys=True,indent=2))'   > "$evidence/origins.log"
for round in 1 2; do
  cargo run --locked --manifest-path "$app/Cargo.toml" -- "$evidence/orders.sqlite" "$evidence/receipt-$round.txt" "$round" |
    tee "$evidence/run-$round.log"
done
fingerprint > "$evidence/source-after.sha256"
cmp "$evidence/source-before.sha256" "$evidence/source-after.sha256"
printf 'Order attachment evidence retained at %s\n' "$evidence"
