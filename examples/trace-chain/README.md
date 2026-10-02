# Generated graph Trace Chain example

This small SQLite example exercises the generated Q, E and audited Mutation
APIs against the local runtime checkout. One save mixes root and child updates,
two inserts, and a child marked for deletion. Assertions observe the actual
emitted commands, physical SQL metadata, canonical safe SQL logs, and committed
App Audit Sink events. The tests never supply expected trace frames to execution.

The logical `Order` in the conformance design is modeled as `customer_order`
because `order` is a reserved SQL word. `CustomerOrder`, `OrderItem`, `Payment`,
`PaymentAttempt`, and `Shipment` are the five business entity types; `Platform`
is the bootstrap root. IDs are assigned by the generated/runtime creation path,
not fixed in test code. Different entity types can legitimately receive the same ID.

## Run against local source

```bash
cd /path/to/teaql-rs
export CARGO_TARGET_DIR=/path/to/shared-cargo-target
bash examples/trace-chain/verify.sh
```

The script runs twice on one persistent SQLite database without cleanup between
runs. It retains both logs and prints their directory, the database URL, and a
relative-path SHA-256 manifest digest for the unchanged generated library.
An existing database may be supplied with `TEAQL_TRACE_CHAIN_DATABASE`; retained
logs may be directed with `TEAQL_TRACE_CHAIN_EVIDENCE_DIR`. The application adds
fresh graphs on each start and never resets tables.

`Cargo.toml` resolves every TeaQL dependency to this repository through local
paths and patches. This is the local-source gate, not an internal/public artifact
verification. Keep the lockfile for repeatable dependency resolution.

## What is asserted

| Boundary | Assertion |
| --- | --- |
| Mixed save | Six mutation items use one audited root save; PaymentAttempt inherits the Payment reason; Shipment and the deleted item retain only their own branch reason |
| Typed identity | Command and physical metadata IDs are known and matched by entity type plus ID; committed lineage uses the same assigned IDs |
| Query and Expression | Generated Q loads the committed reverse lists, and E verifies description, list sizes and the remaining item ID |
| Three relation levels | Generated Q loads PaymentAttempt → Payment → CustomerOrder → Platform; all four physical query metadata records and safe SQL records inherit one comment and purpose and retain the ordered relation route |
| Concurrent saves | Two generated graphs share one Context, meet at a test-only barrier before transaction begin, and yield after acquiring a transaction; each command, SQL metadata item and committed audit event retains its own reasons |
| Provider failure | A test-owned faulty ID allocator causes a real SQLite primary-key conflict after the root INSERT; attempted command and failure SQL metadata retain branch lineage, both writes roll back, and no committed audit event is delivered |
| Readback failure | A transport probe delegates the real INSERT unchanged to SQLite, then rejects the following readback; successful write metadata and failed readback metadata remain separate, the graph rolls back and no committed audit event is delivered |
| Generated library | Its file-content manifest digest is unchanged across application verification |

Canonical SQL Trace Path and audit lineage remain distinct. The accepted v1
canonicalization omits Entity IDs when rebuilding a path; command/physical
metadata identity is checked before that projection. Deleted audit field
projections need not contain a new ID value; this fixture checks the deleted
child's typed identity in its lineage rather than inventing a field value.

The concurrency scenario prepares entity IDs before the overlapping saves. It
does not prove that raw synchronous ID allocation or schema operations are safe
while another transaction holds the SQLite connection. Lower-ledger late ID
assignment, complete ledger-specific
override semantics, and immutable internal-artifact replay remain separate gates.

## Regenerate without editing the library

The generator test evaluates [model.xml](model.xml), generates the library and
application instructions, and retains current object/field Assist outputs. It
rejects model errors before generation. From the generator checkout:

```bash
CARGO_TARGET_DIR=/path/to/shared-cargo-target \
  mvn -B -pl generator -am \
  -Dtest=RustNamingTest,RustTraceChainExampleGenerationTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dteaql.rs.dir=/path/to/teaql-rs test
```

The model explicitly selects `teaql_rs_version="5.0.5"`. This is the dependency
label; local patches select the source revision under test. Read the generated
[AGENTS.md](AGENTS.md) and only the relevant retained Assist before changing
application code. Do not read generated library source for API discovery or
hand-patch it. `src/observation.rs` is a framework SPI test observer: it delegates
the exact generated executor and its transactions, and never creates business
mutations or fabricated metadata.

Generated Q retains its typed executor binding. The fixture therefore also
installs that executor's opt-in `with_query_metadata_observer` during trusted
Context assembly. This callback checks successful provider metadata in memory;
it is disabled by default and is not a safe operator log sink. Raw binds must
never be logged or exposed through this SPI. Mutation failures use the existing
runtime diagnostic callback, still delegated to the safe Context sink.

The failure probes also leave the generated library unchanged. `src/failure.rs`
installs a deliberately faulty runtime ID-generator SPI only after normal fixture
setup. `src/readback_transport.rs` wraps the same SQLite provider and the generated
model schema below the standard SQL compiler; it injects a labeled transport error
only after an actual successful write. The readback error is synthetic, not a claim
that SQLite spontaneously failed. Neither probe injects expected trace nodes, edits
SQL, writes business data through raw commands, or sends rolled-back audit facts to
the committed sink.

Owning issues: [Rust #239](https://github.com/teaql/teaql-rs/issues/239),
[generator #251](https://github.com/teaql/teaql-code-gen/issues/251), and
[Rust 5.x Assist #252](https://github.com/teaql/teaql-code-gen/issues/252).
