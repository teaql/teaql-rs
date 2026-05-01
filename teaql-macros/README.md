# teaql-macros

Derive macros for TeaQL Rust entities.

`teaql-macros` provides `#[derive(TeaqlEntity)]`, which generates TeaQL entity
metadata and typed record mapping implementations for Rust structs.

## Example

```rust
use teaql_macros::TeaqlEntity;

#[derive(Clone, Debug, TeaqlEntity)]
#[teaql(entity = "Merchant", table = "merchant_data")]
struct Merchant {
    #[teaql(id, column = "id")]
    id: u64,

    #[teaql(version, column = "version")]
    version: i64,

    #[teaql(column = "name")]
    name: String,
}
```

Relation metadata can be declared on fields:

```rust
use teaql_core::SmartList;
use teaql_macros::TeaqlEntity;

#[derive(Clone, Debug, TeaqlEntity)]
#[teaql(entity = "Employee", table = "employee_data")]
struct Employee {
    #[teaql(id)]
    id: u64,
}

#[derive(Clone, Debug, TeaqlEntity)]
#[teaql(entity = "Merchant", table = "merchant_data")]
struct Merchant {
    #[teaql(id)]
    id: u64,

    #[teaql(relation(target = "Employee", local_key = "id", foreign_key = "merchant_id", many))]
    employees: SmartList<Employee>,
}
```

Use this crate with `teaql-core` and `teaql-runtime` for typed repository
results and graph persistence.
