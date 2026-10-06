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

The script first runs fifteen native allocation tests, six native identity
graph tests, four numeric-partition tests (including actual cold/warm cache
execution), and thirteen relation-membership tests twice,
then requires the generated scenario markers and all twenty structured aggregate
observations twice on one persistent database without cleanup. It retains all ten logs and prints their
directory, the database URL, and a relative-path SHA-256 manifest digest for the
unchanged generated library.
It also requires the generated Checker-overlap marker in both logging modes on
each run. For the focused same-database case, set
`TEAQL_TRACE_CHAIN_SCENARIO=checker-overlap` when running the example directly.
The generated ledger-precedence marker is also required in both logging modes;
`TEAQL_TRACE_CHAIN_SCENARIO=ledger-override` selects that focused case.
An existing database may be supplied with `TEAQL_TRACE_CHAIN_DATABASE`; retained
logs may be directed with `TEAQL_TRACE_CHAIN_EVIDENCE_DIR`. The application adds
fresh graphs on each start and never resets tables.

`TEAQL_TRACE_CHAIN_SCENARIO=aggregation` selects the focused aggregate case.
Four root/nested × logging combinations check related counts and ordinary
numeric grouping; eight additionally load the reverse members with visible or
filtered forward detail. Four independent full-detail reads must not widen the
earlier NotLoaded views. The related count is 1 while list membership remains 2.
Logging-on compares complete original request metadata and runtime-owned safe
SQL paths; logging-off checks successful Q/E results and intentionally unrecorded
diagnostics. It never forces capture or supplies expected trace frames.

The native regressions cover inverse convenience wiring overriding explicit
forward projections, future aggregate bindings leaking into the first parent
SQL log, and nested typed selections losing their related metrics. Generated
entities expose aliases through `Entity::into_values`, not `BaseEntity::dynamic_i64`.
Runtime and Assist changes belong to the existing issues linked below. Source
verification is not an immutable Registry replay or a public release claim.

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
| Shared read-only reference | One bounded generated Q loads two orders whose E Platform values are pointer-identical. Their mutation ledgers stay independent, each composes only its own new item, and overlapping saves write exactly four items with isolated command/SQL/audit reasons. The shared Platform snapshot, ledger and version stay unchanged; generated Q/E reloads both commits |
| Real generated Checker overlap | Two OS threads save independently owned graphs through one original Context and pointer-shared immutable Platform. A bounded callback rendezvous delegates the generated Checker unchanged, holding actual empty/Required result sets concurrently. Only the valid root/item reach commands, SQL and committed audit; the rejected root's database version/value and the Platform remain unchanged, and its missing-name child is absent. Both logging modes run |
| Complete ledger-chain precedence | Before BEGIN, a read-only observer captures planner-owned chains. Insert/update/delete then emit exactly those complete chains at commands, physical SQL and committed safe audit; fallback is not appended, an unannotated sibling remains root-only, and committed scopes clear. Generated Q/E checks stored names, foreign keys, optimistic versions and deletion. Both logging modes run |
| Provider failure | A test-owned faulty ID allocator causes a real SQLite primary-key conflict after the root INSERT; attempted command and failure SQL metadata retain branch lineage, both writes roll back, and no committed audit event is delivered |
| Readback failure | A transport probe delegates the real INSERT unchanged to SQLite, then rejects the following readback; successful write metadata and failed readback metadata remain separate, the graph rolls back and no committed audit event is delivered |
| Successful readback | Generated Q/E and one root save create and then update a root/item graph. Each changed row returns its actual write followed by a SELECT, retaining root and branch reasons at physical metadata and the safe SQL sink; four writes and four reads produce only four committed audits |
| Scalar streaming | Generated Q/E retains owned intent across deferred consumption and overlapping requests. SQLite serializes them on one connection lease: Drop releases the active cursor before safe cancellation logging, and the waiting stream and query then complete. Terminal counts are 1 cancelled / 3 successful; saving one fully loaded streamed row never saves or clears its sibling's mutation |
| Page and COUNT | Generated Q/E returns total=3, offset=1, two independently editable roots and their filtered items. The model explicitly masks `order_item.name`. COUNT retains removed relation bindings for local classification, without executing them; COUNT, root and child intent all redact the secret while preserving the ordinary root filter. Saves retain per-row versions and reasons |
| Generated database IDs | The real SQLite ID-space generator assigns root/item IDs during `new_entity`, before transaction begin; save does not allocate again, and commands, physical SQL, committed audits and reloaded generated Q/E agree on both identities and the foreign key |
| Native transaction allocation | Eleven tests include both in-process allocation checks and database-backed allocation inside a real transaction; declared forward/reverse ID references resolve to the allocated parent, an unrelated numeric zero is unchanged, and failed writes retain typed lineage without committing rows or audit |
| Native numeric grouping | Root scalar partitions and loaded relation window/probe queries preserve GROUP BY/HAVING counts, use grouping keys for Top-N tie-breaking, and retain only real relation edges; first-root intent masks future child secrets, logging-off returns the same data, and the next independent request is isolated |
| Generated library | Its file-content manifest digest is unchanged across application verification |

Canonical SQL Trace Path and audit lineage remain distinct. The accepted v1
canonicalization omits Entity IDs when rebuilding a path; command/physical
metadata identity is checked before that projection. Committed SafeAuditEvent
retains the independent positive-integer `entity_id` alongside `entity`, even
when changed fields omit ID and the entire responsibility chain belongs to the
parent. No child reason or ID field change is invented. After the normative
graph, an unannotated child is marked for deletion and saved through its clean
parent; exactly that child is identified at command/metadata/audit boundaries,
its chain remains root-only, and bounded generated Q confirms it is hidden.
The verifier requires `TC-MUT-15 SAFE TARGET PASSED` in both retained runs.

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

The shared-reference scenario uses Rust's actual flat identity graph, not a
Java-style object setter or entity cloning. Immutable relation snapshots may be
shared; mutation ownership must not be. Native regressions exercise both the
macro hydration boundary and same-ID/different-type snapshot versions. A
temporary negative control replacing fresh ledger ownership with a shared clone
fails the hydration regression; it is restored before acceptance. The generated
scenario updates a version-qualified description on each replay, so a second
run proves actual new writes rather than a no-op. It runs before the deliberately
broken allocator fixture; each generated invocation is bounded by 180 seconds.

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
The complete-chain precedence scenario below closes that generated assertion;
immutable internal-artifact replay remains a separate gate.

`src/ledger_override.rs` observes existing runtime-produced scopes, not a new
public API for injecting arbitrary audit chains. The once-only pre-BEGIN probe
does not mutate the graph, transaction, or request. One annotated child and one
unannotated sibling are saved through create, update and soft-delete phases.
Changing the native resolver to append fallback to the specific chain makes the
actual emitted-command assertion fail (two root reasons instead of one). That
deliberate negative control is restored before the two-run acceptance; it is
not an existing product defect. A separate native unit test covers empty-chain
fallback. The generated scenario neither supplies expected trace nodes nor
claims an external ledger-override setter. The same DB retains earlier attempts.

`src/checker_overlap.rs` uses current Runtime Customization, Create, Update,
Query and Expression Assist. Its registry wraps `checker_registry()` and retains
every model Checker; it neither manufactures failures nor changes values,
locations or results. The invalid new item deliberately omits its required name,
which the actual generated typed Checker reports at `order_item_list[0].name`.
Blank-string validation is not assumed. Thread IDs, original Context pointer and
peak callback activity are asserted, and the rendezvous times out after 15 seconds.
This is stronger than joining synchronous callbacks on one async thread.
The dedicated `RustCheckerOverlapAcceptanceTest` producer fixture regenerates and
hashes library bytes without reading generated sources; do not use the older
source-inspecting regeneration test for this acceptance path.

Successful readback diagnostics are physical children of the existing mutation
summary. Runtime sinks visit those children once; affected-row totals and
committed audit delivery still describe mutations only. Native tests cover
observer-present and observer-absent execution, native batch item grouping,
guard retention, no-match and hard-delete cases, sibling-secret masking and
concurrent independent native batches. No extra SELECT is issued for tracing.
The generated library remains unchanged. The verifier requires
`TC-REQ-10 SUCCESSFUL READBACK PASSED` on both retained starts.
Broader entry-point/privacy/provider acceptance and immutable
artifact consumption remain separate gates; this is not complete Trace Chain.

`src/paging.rs` uses current field/query/list-page Assist. The count transform now
belongs to the runtime's `PurposedSelectQuery`: it retains a private diagnostic
source, never serializes it, and does not print that extra source in `Debug`.
Runtime query-tree classification visits each nested query with its own compiler
metadata before root logging. It executes no extra SQL and stores no provenance
on the shared Context. Native SQLite tests cover borrowed/owned count execution,
real SELECT failure, ordinary-value visibility, no cross-request contamination,
logging-off validation and a counterexample reproducing the old source-discarding
transform. The generated library requires the matching local runtime revision;
the unchanged 5.0.5 label is not evidence of a published compatible artifact.

`src/streaming.rs` uses current `rust-assist-list-page/customer_order` and
expression/update Assist. It rejects missing/blank comments and unsupported
relation streaming without SQL, and a never-polled stream produces no SQL fact.
Generated `ServiceRuntimeExecutor` delegates terminal observations to the native
executor; the runtime safe sink is called before optional trusted observation.
This checkpoint does not add graph streaming or parallel SQLite connections.
Native counts are provider-delivered rows, not application-processed rows;
chunk size 1 makes the early-close assertion exact. Expected blank-intent panic
diagnostics are retained and caught only by the negative tests.

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

The loaded graph privacy scenario (`src/graph_privacy.rs`) adds two successive
updates and a marked deletion. A root reason quotes a child's old/new masked
name; both SQL diagnostics and committed audit must hide those values, while
generated Q/E verifies the actual stored values. A later independent query keeps
its own intent unchanged. The verifier requires the `TC-REQ-16 LOADED GRAPH
PRIVACY PASSED` marker. Loaded snapshots and pending values are captured by the
runtime before graph execution, not by a generated helper or shared Context.

Trusted `MutationRequest` and `RawAuditEvent` now carry private diagnostic
provenance. Application integrations should construct audit facts through their
existing constructors, not struct literals; safe audit output has no such field.
No generated API or generator update is required for this checkpoint.

`src/graph_privacy_failure.rs` combines loaded private values with failure and
retry. Both real UPDATEs succeed, then the child's readback is deliberately
rejected. The example requires safe failure diagnostics, zero committed audit,
unchanged stored values/versions, and a successful fresh-query retry with exactly
one version increment. Independent intent must remain unchanged afterward.
The controlled verifier requires `TC-REQ-16 GRAPH PRIVACY ROLLBACK PASSED`.
For a focused diagnostic run, set
`TEAQL_TRACE_CHAIN_SCENARIO=loaded-privacy-rollback`; the complete gate requires
all other scenarios too, so that selector cannot silently skip acceptance.
