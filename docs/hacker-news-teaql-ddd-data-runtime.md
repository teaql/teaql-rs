# Building a DDD Data Runtime with Generated Typed Queries in Rust

Suggested Hacker News title:

```text
Building a DDD data runtime with generated typed queries in Rust
```

Alternative titles:

```text
TeaQL: a DDD-oriented data runtime for Rust and Java-style domain models
Generating typed query APIs from a domain model in Rust
```

Avoid titles like:

```text
Show HN: TeaQL, the best ORM for DDD applications
```

That makes the project sound like a product pitch. This article works better as
a technical write-up than as a launch announcement.

## Article Draft

I have been working on TeaQL, a data layer for applications where the domain
model is the center of the system rather than a thin mapping over tables. The
current Rust implementation is a rewrite of ideas from the Java TeaQL stack, but
with a smaller scope: PostgreSQL, SQLite, a Rust-native query AST, generated
typed APIs, and no web framework dependency.

The easiest way to describe the goal is this: instead of writing most
application data access as hand-made repository methods or raw SQL fragments,
the domain model generates a crate with entity types, relation metadata, query
builders, checker hooks, behavior hooks, and graph-save entrypoints. Application
code then works with a higher-level API:

```rust
let platforms = Q::platforms()
    .select_merchant_list_with(
        Q::merchants()
            .select_name()
            .which_names_contain("TeaQL"),
    )
    .execute_for_list(&ctx)
    .await?;
```

This is not meant to replace every way of using SQL from Rust. If a service is
mostly carefully tuned SQL, direct `sqlx` is probably a better fit. If the
preferred abstraction is an ORM with a large Rust ecosystem, Diesel or SeaORM
will be more familiar. TeaQL is aimed at a different case: large domain models
where repeated relation loading, graph persistence, validation, and statistics
queries become their own layer of application logic.

## Why Generate the API?

The original pressure came from systems where the same entity model needed to
support several kinds of behavior:

- ordinary list/detail queries;
- nested relation loading, including paths such as `merchant.platform` or
  `platform.merchant_list`;
- graph writes where a parent object and children are committed together;
- additive schema bootstrap for development and tests;
- checker/validation logic that can inspect and fix entities;
- simple and grouped statistics;
- JSON serialization and JSON-expression style search.

You can build all of that by hand, but the code tends to become repetitive in
two places. First, relation names and field names are repeated across query
methods, repository code, and validation code. Second, application developers
end up switching between typed domain objects and untyped row maps. TeaQL tries
to keep the generated surface typed, while letting the runtime keep a generic
query and graph model internally.

For example, a generated service crate exposes `Q::platforms()` and
`Q::merchants()` rather than asking application code to construct
`SelectQuery::new("Platform")` directly. Low-level query objects still exist,
but they are not the normal application-level API.

## What the Rust Runtime Contains

The Rust workspace is split into small crates:

- `teaql-core`: values, records, entity descriptors, query AST, expressions,
  commands, and `SmartList<T>`;
- `teaql-sql`: SQL compilation and dialect-neutral compiled query types;
- `teaql-runtime`: `UserContext`, repository resolution, behavior hooks,
  checker hooks, graph writes, relation enhancement, events, and optional SQLx
  executors;
- `teaql-macros`: `#[derive(TeaqlEntity)]` for descriptors and typed
  record/entity mapping;
- `teaql-dialect-sqlite` and `teaql-dialect-pg`: placeholder and quoting rules.

Generated crates sit above those runtime crates. A generated CRM/ERP service,
for example, exports entities such as `Platform` and `Merchant`, a `Q` query
facade, behavior skeletons, checker skeletons, repository registration, and
runtime module assembly helpers.

## Query Construction

TeaQL has a generic query AST, but generated code provides a domain-specific
facade. Instead of this in application code:

```rust
let query = SelectQuery::new("Merchant")
    .project("id")
    .project("name")
    .filter(Expr::contains("name", "tea"));
```

the generated API can expose:

```rust
let merchants = Q::merchants()
    .select_name()
    .which_names_contain("tea")
    .order_by_create_time_desc()
    .page(1, 20)
    .execute_for_list(&ctx)
    .await?;
```

Relation loading uses the same style:

```rust
let platforms = Q::platforms()
    .select_merchant_list_with(
        Q::merchants()
            .select_name()
            .select_platform(),
    )
    .execute_for_list(&ctx)
    .await?;
```

One detail that mattered in the implementation: when a child is attached to a
parent relation list, the reverse object relation should be populated too. In
the example above, each merchant in `platform.merchant_list` should have its
`platform` relation set. Otherwise, the result is typed but not really a domain
object graph.

## Graph Writes

TeaQL has a `save_graph` path for committing complex objects. In generated
crates, application code can call a typed save helper:

```rust
merchant
    .update_name("TeaQL Merchant")
    .update_platform_id(1_u64)
    .save(&ctx)
    .await?;
```

Internally, the runtime turns that object into a graph plan. The plan classifies
nodes by entity and operation: create, update, delete/remove, or reference. It
then batches compatible work where possible and runs the graph write inside a
transactional executor.

The interesting part is not just inserting children. Updating a graph means
answering questions like:

- is this child new, already present, a reference, or explicitly removed?
- should missing children be soft-deleted or left alone?
- should a relation write attach a foreign key, or is it detached?
- if a reference points at a deleted row or the wrong version, should the graph
  write fail?

The Rust runtime currently supports nested create/update graph writes,
reference-only nodes, explicit remove nodes, keep-missing relation metadata,
duplicate child-id rejection, and transaction rollback for SQLite and
PostgreSQL SQLx executors.

## Schema Bootstrap

For local development and generated-service tests, TeaQL can bootstrap a schema
from entity descriptors:

```rust
ctx.ensure_sqlite_schema().await?;
```

The current scope is intentionally conservative. It creates missing tables and
adds missing columns. It does not try to be a destructive migration tool: no
column drops, no primary-key rebuilds, and no automatic type rewrites. That line
is important because generated domain models change frequently; the bootstrap
path should be safe enough for local and CI use, not pretend to replace a real
production migration process.

The same ensure path also seeds TeaQL constants where the model declares them,
which matters when a generated service evolves over many small changes.

## Checkers and Domain Validation

One Java TeaQL idea that carried over well is checker registration. A checker is
not just a validator that rejects a row. It can inspect an object, add
structured check results, and sometimes fix fields before persistence.

The generated Rust checker support now lets application code write typed
checker logic instead of manually reading from a `Record`:

```rust
impl MerchantCheckerLogic for MerchantNameChecker {
    fn check_and_fix_merchant(
        &self,
        _ctx: &UserContext,
        entity: &mut Merchant,
        status: CheckObjectStatus,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        if status.is_create() {
            self.required_text(&entity.name(), "name", location, results);
        }

        self.min_string_length(&entity.name(), "name", 3, location, results);

        if entity.name() == "fix" {
            entity.update_name("fixed");
        }
    }
}
```

The runtime still stores the common checker interface at the record level, but
the generated adapter maps records into typed entities before calling the
checker. That keeps the public application code close to the domain model while
preserving a generic runtime path.

## Statistics

TeaQL queries can carry aggregate projections and relation aggregate metadata.
The current runtime supports simple aggregates, grouped aggregates, Decimal
results for SQL aggregate output, relation count/statistic attachment, and
database-column-to-entity-property mapping for relation aggregate keys.

The generated `Q` APIs can express both simple statistics and relation
statistics. For example, a service can count child rows from a parent query
without asking application code to hand-build the join every time.

This is an area where the design is useful but still evolving. The runtime has
working SQL and memory paths for the core cases, while broader Java parity still
needs more work around memory subqueries and richer relation aggregate shapes.

## Tradeoffs

The most obvious tradeoff is generated code. TeaQL generates a lot of Rust. That
is a cost: compile time, large diffs, and the need to keep templates disciplined.
The benefit is that application code gets a stable, typed facade over a large
domain model. In the projects this was designed for, that tradeoff is preferable
to repeatedly hand-writing repository and relation APIs.

Another tradeoff is that the runtime is not purely compile-time checked. The
generated APIs are typed, but the runtime still has a generic query AST, record
model, and descriptor registry. That gives it flexibility for dynamic
projections, aggregate rows, JSON-style search, and graph planning, but it means
some mistakes are caught by generated crate tests rather than by Rust types
alone.

The final tradeoff is scope. The Rust rewrite is not trying to clone every Java
TeaQL feature or support every database. PostgreSQL and SQLite are enough for
now. Web rendering, GraphQL integration, and broad database dialect support are
outside the current Rust scope.

## What Works Today

The current Rust runtime and generated crate tests cover:

- SQLite schema bootstrap and additive column changes;
- PostgreSQL schema bootstrap with SQLx;
- CRUD, optimistic locking, soft delete, and recover;
- typed entity fetch into `SmartList<T>`;
- nested relation enhancement;
- complex object commit through graph writes;
- transaction rollback for graph writes;
- generated `Q` APIs against SQLite;
- typed checker adapters from generated crates;
- JSON serialization and JSON-expression search paths;
- simple aggregates, grouped aggregates, and relation aggregate statistics.

The public examples can be run with:

```bash
cargo run -p teaql-examples --bin sqlite_schema_crud
cargo run -p teaql-examples --bin sqlite_relations_graph
```

The first command shows schema bootstrap and CRUD against in-memory SQLite. The
second saves an object graph and reloads nested relations.

## What Is Not Done

The biggest gaps are:

- more complete memory repository parity for relation enhancement and subquery
  execution;
- richer checker semantics, especially nested typed object locations and
  domain-specific labels;
- richer event payloads with old/new values and typed snapshots;
- more value types such as UUID and bytes;
- a decision on whether Rust needs a higher-level service layer above the
  repository/runtime APIs.

Those gaps are real. I would rather keep them visible than make the project look
more finished than it is.

## Why I Think This Shape Is Worth Exploring

Most Rust database libraries are good at one of two layers: explicit SQL, or a
database-centric ORM. TeaQL is exploring a third shape: generated domain APIs
over a generic runtime that understands entity graphs, relation enhancement,
validation, and statistics.

That shape will not fit every codebase. It is most useful when the model is
large enough that the generated API becomes an asset, and when the team wants
the same domain semantics to appear in queries, graph writes, checkers, and
schema bootstrap.

I am especially interested in feedback from people who have built large
business systems in Rust, Java, or both. Does this generated-domain-runtime
approach solve a real problem in your projects, or would the generated surface
area become too much weight? Where would you draw the line between generated
domain API and explicit SQL?

## Suggested First HN Comment

I am the author of TeaQL. The article is a technical write-up of the Rust
rewrite and generated service API, not a polished product launch.

The shortest local demo is:

```bash
cargo run -p teaql-examples --bin sqlite_relations_graph
```

That runs against in-memory SQLite, bootstraps the schema, saves an object graph,
and reloads nested relations. The generated `Q::platforms()` style API is tested
through a generated service crate, but the public examples in `teaql-rs` use the
runtime crates directly so the mechanics are easier to inspect.

I am looking for feedback on the shape of the abstraction: generated typed
domain API above a generic query/graph runtime. In particular, I would like to
hear where people think this belongs compared with direct `sqlx`, Diesel,
SeaORM, or a more conventional repository layer.

## Launch Checklist

- The README first screen explains what TeaQL is and what it is not.
- At least one no-server demo command works.
- The article includes technical details and tradeoffs rather than only feature
  lists.
- The author can stay online for several hours after posting to reply with
  concrete details.
- Do not ask anyone to upvote or comment.
- Do not paste generated replies into the HN thread.
