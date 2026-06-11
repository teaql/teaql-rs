#!/bin/bash
set -e

CRATES=(
  "teaql-core"
  "teaql-data-service"
  "teaql-sql"
  "teaql-macros"
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
  until cargo publish -p "$crate" --allow-dirty --no-verify; do
    echo "Publishing $crate failed, likely due to crates.io index sync. Retrying in 5 seconds..."
    sleep 5
  done
  echo "$crate published successfully!"
done

echo "All crates published!"
