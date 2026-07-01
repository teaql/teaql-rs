#!/bin/bash
set -e

# teaql-core and teaql-macros have a circular dependency:
#   teaql-macros → teaql-core (hard dep)
#   teaql-core → teaql-macros (dev-dep)
# cargo publish resolves dev-deps during packaging, so we must temporarily
# remove the dev-dep from teaql-core before publishing it.

CORE_CARGO="teaql-core/Cargo.toml"

# --- temporarily strip the circular dev-dep ---
if grep -q '^\[dev-dependencies\]' "$CORE_CARGO"; then
  echo "Temporarily removing teaql-macros dev-dep from teaql-core for publishing..."
  sed -i.bak '/^\[dev-dependencies\]/,$d' "$CORE_CARGO"
fi

cleanup() {
  if [ -f "${CORE_CARGO}.bak" ]; then
    echo "Restoring teaql-core/Cargo.toml..."
    mv "${CORE_CARGO}.bak" "$CORE_CARGO"
  fi
}
trap cleanup EXIT

CRATES=(
  "teaql-core"
  "teaql-macros"
  "teaql-data-service"
  "teaql-sql"
  "teaql-runtime"
  "teaql-provider-meilisearch"
  "teaql-provider-mysql"
  "teaql-provider-postgres"
  "teaql-provider-sqlite"
  "teaql-cache-integration-redis"
  "teaql-web-integration-axum"
)

for crate in "${CRATES[@]}"; do
  echo "Publishing $crate..."
  until OUT=$(cargo publish -p "$crate" --allow-dirty --no-verify 2>&1) || echo "$OUT" | grep -q "already exists"; do
    echo "$OUT"
    echo "Publishing $crate failed, likely due to crates.io index sync. Retrying in 5 seconds..."
    sleep 5
  done
  echo "$crate published successfully!"
done

echo "All crates published!"
