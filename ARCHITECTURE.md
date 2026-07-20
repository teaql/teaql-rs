# TeaQL Architecture

This document provides a high-level overview of the TeaQL project architecture.

## Core Components

1. **teaql-core**: Contains the core trait definitions, metadata structures, and shared types.
2. **teaql-runtime**: The execution engine that handles query generation, AST evaluation, and connection management.
3. **teaql-sql**: Abstract SQL generation and dialect translation for different database engines.
4. **teaql-data-service**: The high-level service layer that provides unified APIs for entity mutation and querying.

## Database Providers

TeaQL abstracts database-specific implementations into separate provider crates:
- `teaql-provider-postgres`
- `teaql-provider-mysql`
- `teaql-provider-sqlite`
- `teaql-provider-meilisearch`

Each provider implements the execution traits required by the runtime, ensuring that the core logic remains database-agnostic.

## Cloud Integration

TeaQL provides cloud-native capabilities for embedding Rust services into Java microservice architectures:

- `teaql-cloud-core` — Backend-agnostic trait definitions (ServiceRegistry, ServiceDiscovery, ConfigSource, HealthIndicator, MetricsCollector)
- `teaql-cloud-actuator` — Spring Boot Actuator-compatible HTTP endpoints (health, info, metrics) via Axum
- `teaql-cloud-nacos` — Nacos v2 gRPC implementation (wraps `nacos-sdk =0.8`)
- `teaql-cloud-starter` — One-line bootstrap combining all cloud capabilities

```
┌─────────────────────────────────────────────────────┐
│                  teaql-cloud-starter                 │
│  (CloudApp builder — one-line bootstrap)            │
├──────────────────┬──────────────────┬───────────────┤
│ teaql-cloud-     │ teaql-cloud-     │ teaql-cloud-  │
│ actuator         │ nacos            │ core          │
│ (Axum endpoints) │ (Nacos v2 gRPC)  │ (Traits)      │
└──────────────────┴──────────────────┴───────────────┘
```

Design document: [docs/2026-07-20-cloud-integration-design.md](docs/2026-07-20-cloud-integration-design.md)

## Security Design Principles
- **Input Validation**: All query parameters are strongly typed and sanitized by the underlying providers.
- **Memory Safety**: Leveraging Rust's ownership model to prevent memory leaks and data races.
- **Separation of Concerns**: The AST and query builders are strictly separated from database execution logic.
