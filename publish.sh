#!/bin/bash
set -e

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
