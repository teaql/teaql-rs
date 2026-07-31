# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
