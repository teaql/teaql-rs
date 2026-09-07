# teaql-core

Core model and query primitives for TeaQL Rust.

`teaql-core` contains the stack-neutral pieces used by the rest of the TeaQL
Rust crates:

- entity and relation metadata
- typed values and records
- select, insert, update, delete, and recover command models
- expression builders and filters
- ordering, grouping, projection, aggregation, and relation aggregate models
- `SmartList<T>` collection metadata
- `TeaqlEntity` and `Entity` traits for typed row mapping
- `SafeExpression` helpers for null-safe value access

## Example

```rust
use teaql_core::{Expr, SelectQuery};

let query = SelectQuery::new("Merchant")
    .select("id")
    .select("name")
    .filter(Expr::eq("name", "TeaQL"))
    .page(1, 20);
```

Use this crate when you need TeaQL metadata, query AST, entity traits, or
in-memory value conversion without choosing a SQL dialect or runtime executor.

## Local dynamic-search schema drift

`dynamic_search::normalize_dynamic_search` accepts a local UI-search JSON
envelope (`filter`, `orderBy`) and application-owned `SearchModels`. Unknown
fields and relation paths discard the complete clause and return value-free
`DYNAMIC_SEARCH_UNKNOWN_FIELD` warnings. With no warning callback, warnings are
logged as JSON to stderr. Malformed JSON, invalid operators/types and trusted
context controls remain fatal; TFP validation is unchanged.

`dynamic_search::merge_dynamic_search` compiles validated clauses through trusted
native `Expr`/`OrderBy` bindings and clones the existing scoped query. Its filters
are ANDed with the original predicate, ordering is appended, and hard limits,
pagination and trace intent are retained. Bindings must preserve authorization
inside related queries too. Validation or binding failure emits no partial
warnings and never mutates the original query.

Metadata types: `string`, `integer`, `number`, `boolean`, `date` (`yyyy-MM-dd`),
`timestamp` (integer epoch milliseconds), and `decimal` (use strings for exact
digits). Operators: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$notIn`,
and string `$contains`. Merge uses a 100-clause limit; normalization accepts an
explicit trusted clause limit. Both bound paths to 16 segments and IN lists to
1,000 values. Automatic generated bindings are not provided by this adapter.

Tests: `teaql-core/tests/dynamic_search.rs` and
`teaql-provider-sqlite/tests/dynamic_search.rs`.

## Workspace links

This crate is part of the `teaql-rs` workspace:

- `teaql-sql` compiles `teaql-core` query models into SQL.
- `teaql-runtime` executes repository operations and graph persistence.
- `teaql-macros` derives `TeaqlEntity` implementations.
