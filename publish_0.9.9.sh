#!/bin/bash
set -e

# Before running this, ensure you are authenticated:
# cargo login --registry teaql <YOUR_NEXUS_TOKEN>
# cargo login <YOUR_CRATES_IO_TOKEN>

echo "Step 1: Temporarily removing circular dev-dependency to allow publishing"
sed -i 's/teaql-macros = .*/#teaql-macros = /g' teaql-core/Cargo.toml

echo "Step 2: Publishing to private registry 'teaql'"
cargo publish -p teaql-core --registry teaql --allow-dirty
sleep 2
cargo publish -p teaql-macros --registry teaql --allow-dirty
sleep 2

# Restore the dev-dependency
git checkout teaql-core/Cargo.toml

cargo publish -p teaql-sql --registry teaql --allow-dirty
sleep 2
cargo publish -p teaql-runtime --registry teaql --allow-dirty
sleep 2
cargo publish -p teaql-provider-rusqlite --registry teaql --allow-dirty
cargo publish -p teaql-provider-sqlx-postgres --registry teaql --allow-dirty
cargo publish -p teaql-provider-sqlx-sqlite --registry teaql --allow-dirty
cargo publish -p teaql-provider-sqlx-mysql --registry teaql --allow-dirty

echo "Private registry publish complete."

echo "Step 3: Preparing to publish to crates.io (Requires removing registry attribute)"
# Remove `registry = "teaql"` temporarily so Cargo defaults to crates.io
sed -i 's/, registry = "teaql"//g' Cargo.toml

echo "Publishing to crates.io"
sed -i 's/teaql-macros = .*/#teaql-macros = /g' teaql-core/Cargo.toml
cargo publish -p teaql-core --allow-dirty
sleep 2
cargo publish -p teaql-macros --allow-dirty
sleep 2
git checkout teaql-core/Cargo.toml

cargo publish -p teaql-sql --allow-dirty
sleep 2
cargo publish -p teaql-runtime --allow-dirty
sleep 2
cargo publish -p teaql-provider-rusqlite --allow-dirty
cargo publish -p teaql-provider-sqlx-postgres --allow-dirty
cargo publish -p teaql-provider-sqlx-sqlite --allow-dirty
cargo publish -p teaql-provider-sqlx-mysql --allow-dirty

echo "Restoring Cargo.toml"
git checkout Cargo.toml

echo "All publishing complete!"
