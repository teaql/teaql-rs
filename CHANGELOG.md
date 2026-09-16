# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [5.0.0] - 2026-09-16

Release tracking: [Rust #144](https://github.com/teaql/teaql-rs/issues/144).
The coordinated nine-crate release is available on crates.io, with a signed
[`v5.0.0`](https://github.com/teaql/teaql-rs/releases/tag/v5.0.0) tag on the
verified source commit.

### Changed

- Move the Rust workspace and its intra-workspace crate requirements from
  `4.3.5` to `5.0.0`, including the cloud integration crates.
- Update the retained local Conformance, School, and Order example manifests
  to resolve this checkout's `5.0.0` runtime. Keep the official `4.3.5`
  archive-replay fixtures pinned as historical evidence.
- Treat this as a major compatibility boundary: generated projects pinned to
  `4.x` do not select the local `5.0.0` runtime through Cargo patches.

### Migration and verification

- Regenerate a consumer once the generator adopts `5.0.0`, or explicitly
  update its TeaQL dependency requirements when testing against a local
  checkout. Do not infer a successful 5.0 test from a patch declaration alone;
  inspect `cargo tree` to confirm the selected package versions and source.
- Local verification: `cargo test --workspace --lib --tests --quiet` and
  `scripts/verify-examples.sh` completed with no failures. One Redis-only test
  remains explicitly ignored without `TEAQL_REDIS_URL`.
- Public archive provenance, signed-tag verification, and a clean downloaded
  Order consumer replay passed. Generator default-version adoption remains a
  separate release gate.

## [4.2.2] - 2026-07-31

### Changed
- Simplify README current scope documentation
- Refactor runtime: enforce purposed reads and audited saves

### Fixed
- Fix Linux provider: use public in-memory query engine
- Bump axum dependency to 0.8.9

## [4.2.0] - 2025-07-25

### Added
- teaql-cloud-consul crate with Consul service registry integration
- teaql-cloud-starter crate for one-line cloud bootstrap
- teaql-cloud-nacos crate wrapping Nacos v2 gRPC SDK
- teaql-cloud-actuator crate with health, info, and metrics endpoints
- teaql-cloud-core crate with cloud integration trait definitions
- LargeText semantic type to align with Java implementation
- Core TeaQL Rust implementation
- SQL and Data Service providers

### Fixed
- Remove unused relation key variables in macros
- Ignore flat column names when mapping relation fields
- Implement typed null bindings via AST metadata injection for PostgreSQL
