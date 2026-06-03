# teaql-data-service

Abstract data service executor traits that decouple the TeaQL runtime from
concrete database backends.

This crate defines the interface boundary between the high-level TeaQL runtime
(query planning, graph writes, relation enhancement) and the low-level database
drivers. Backend provider crates (e.g. `teaql-provider-rusqlite`,
`teaql-provider-sqlx-postgres`) implement these traits so the runtime never
depends on a specific database library directly.

## Key Traits

| Trait | Purpose |
|---|---|
| `DataServiceExecutor` | Base trait exposing backend capability flags |
| `QueryExecutor` | Execute `SelectQuery` requests and return typed rows |
| `MutationExecutor` | Execute insert / update / delete / recover mutations |
| `TransactionExecutor` | Begin transactions that combine query + mutation in an atomic scope |
| `SchemaExecutor` | Ensure entity tables and columns exist in the backing store |
| `IdGeneratorExecutor` | Generate unique entity IDs through the backend's id-space mechanism |

## Supporting Types

- `QueryRequest` / `QueryResult` — query envelope with trace chain and execution metadata
- `MutationRequest` / `MutationResult` — mutation envelope supporting single and batch operations
- `SchemaRequest` / `SchemaResult` — schema migration envelope
- `ExecutionMetadata` — timing, affected rows, debug query, and backend request ID
- `DataServiceCapabilities` — declares which executor capabilities a backend supports
- `DataServiceOperation` — enum of operation kinds for metadata tagging
