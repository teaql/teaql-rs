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

The script first runs eleven native allocation tests twice on independent
in-memory SQLite fixtures, then runs seven generated scenarios twice on one
persistent database without cleanup. It retains all four logs and prints their
directory, the database URL, and a relative-path SHA-256 manifest digest for the
unchanged generated library.
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
| Same-type batches | Two OrderItems are included in reverse order, inserted together and later updated with the same changed fields; each command, physical SQL record and committed event retains its own local reason and typed ID; generated Q/E verifies both values and version increments |
| Concurrent saves | Two generated graphs share one Context, meet at a test-only barrier before transaction begin, and yield after acquiring a transaction; each command, SQL metadata item and committed audit event retains its own reasons |
| Provider failure | A test-owned faulty ID allocator causes a real SQLite primary-key conflict after the root INSERT; attempted command and failure SQL metadata retain branch lineage, both writes roll back, and no committed audit event is delivered |
| Readback failure | A transport probe delegates the real INSERT unchanged to SQLite, then rejects the following readback; successful write metadata and failed readback metadata remain separate, the graph rolls back and no committed audit event is delivered |
| Generated database IDs | The real SQLite ID-space generator assigns root/item IDs during `new_entity`, before transaction begin; save does not allocate again, and commands, physical SQL, committed audits and reloaded generated Q/E agree on both identities and the foreign key |
| Native transaction allocation | Eleven tests include both in-process allocation checks and database-backed allocation inside a real transaction; declared forward/reverse ID references resolve to the allocated parent, an unrelated numeric zero is unchanged, and failed writes retain typed lineage without committing rows or audit |
| Generated library | Its file-content manifest digest is unchanged across application verification |

Canonical SQL Trace Path and audit lineage remain distinct. The accepted v1
canonicalization omits Entity IDs when rebuilding a path; command/physical
metadata identity is checked before that projection. Deleted audit field
projections need not contain a new ID value; this fixture checks the deleted
child's typed identity in its lineage rather than inventing a field value.

The batch scenario uses the existing Runtime Telemetry SPI to witness one
successful `OrderItem.batch_insert` and one `OrderItem.batch_update` operation.
Each prepared batch currently issues two physical SQL statements with the same
parameterized shape and separate bindings. This proves the actual grouping
path and per-item trace indexing; it is not a claim of one multi-row SQL
statement or driver-native bulk execution. The observer does not supply reasons,
IDs or trace frames. Application verification includes `TC-MUT-09 PASSED` in
both retained runs.

The concurrency scenario prepares entity IDs before the overlapping saves. It
does not prove that raw synchronous ID allocation or schema operations are safe
while another transaction holds the SQLite connection. Generated creation assigns
IDs early. The separate native [lower-ledger integration tests](../../teaql-provider-sqlite/tests/ledger_allocated_trace.rs)
exercise runtime allocation during save with the actual SQLite ID-space
generator on the transaction-owned physical connection. Database sequence rows,
root/child rows, declared foreign keys, command/physical metadata, safe SQL logs,
committed safe audit and root readback are checked. The graph can allocate both
root and child in one save; only declared ID references are rebound, never
arbitrary numeric values or the source ledger. Forward-only, reverse-only and
bidirectional metadata are exercised.

A real SQLite trigger rejects a child after the root INSERT. Both business
rows and sequence updates roll back, no committed audit is sent, and a retry
after sequence-floor advancement uses new IDs and a new root reason. The
failed SQL log preserves its leaf reason with existing failed-bind redaction.
An exhausted ID space rejects the save instead of silently using an in-process
counter. An explicit transaction also verifies audit delivery only after commit.
The verifier requires each named native test in both runs; a temporary negative
control with relation rebinding disabled fails the real foreign-key check.

These tests do not change generated creation semantics. The generated scenario
uses its normal early allocation path and verifies it separately. They do not
prove safe synchronous allocation by another operation while an unrelated save
owns the connection, or shared ID-space aliases between display and type names.
Complete ledger-specific override semantics and immutable internal-artifact
replay remain separate gates.

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
