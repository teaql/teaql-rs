#!/bin/bash
set -e

CRATES=(
  "teaql-data-service"
  "teaql-sql"
  "teaql-macros"
  "teaql-runtime"
  "teaql-provider-sqlite"
  "teaql-provider-postgres"
  "teaql-provider-mysql"
  "teaql-provider-meilisearch"
  "teaql-cache-integration-redis"
  "teaql-web-integration-axum"
)

for crate in "${CRATES[@]}"; do
  echo "Publishing $crate..."
  until cargo publish -p "$crate" --allow-dirty; do
    echo "Publishing $crate failed, likely due to crates.io index sync. Retrying in 5 seconds..."
    sleep 5
  done
  echo "$crate published successfully!"
done

echo "All crates published!"
