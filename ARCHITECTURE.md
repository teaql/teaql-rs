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

## Security Design Principles
- **Input Validation**: All query parameters are strongly typed and sanitized by the underlying providers.
- **Memory Safety**: Leveraging Rust's ownership model to prevent memory leaks and data races.
- **Separation of Concerns**: The AST and query builders are strictly separated from database execution logic.
