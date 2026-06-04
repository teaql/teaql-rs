#!/bin/bash
set -e

CRATES=(
  "teaql-core"
  "teaql-data-service"
  "teaql-sql"
  "teaql-macros"
  "teaql-runtime"
  "teaql-provider-rusqlite"
  "teaql-provider-sqlx-postgres"
  "teaql-provider-sqlx-sqlite"
  "teaql-provider-sqlx-mysql"
)

for crate in "${CRATES[@]}"; do
  echo "Publishing $crate..."
  until cargo publish -p "$crate"; do
    echo "Publishing $crate failed, likely due to crates.io index sync. Retrying in 5 seconds..."
    sleep 5
  done
  echo "$crate published successfully!"
done

echo "All crates published!"
