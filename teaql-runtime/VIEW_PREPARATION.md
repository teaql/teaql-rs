# View preparation

The trusted terminal, embedded or response boundary calls
`UserContext::prepare_view(input, &options).await` after business work and required
physical transactions complete. `ViewPreparationService` is a normal runtime
service. Installation inserts a typed shared resource, preserves business
registries and starts no listener, migration, key lookup or background job.
The runtime does not depend on a concrete document or Blob implementation.

Install the service with an explicit `ViewAuthorization` into each context and
call `validate_capabilities` after assembly. `view.v1` requires this service;
`view.governed.v1` also requires a document contributor or a compatible existing
application document service and reference provider. Read-only presentations
never invoke document contributors or require document keys.

## Borrowed inputs and authority

`ViewInput` accepts explicit JSON/core values, objects, arrays and borrowed
`ViewEntity` adapters. An adapter supplies identity/revision metadata, already
loaded field names and a separate field reader. Field authorization precedes
that reader. Hidden and masked fields do not read business values; unloaded
fields remain absent, while explicit null remains present. Member aliases are
distinct from canonical policy names. An unreadable entity denies the whole
presentation. Cycles expose only authorized loaded id/version fields.

Adapters must report their actual loaded boundary, avoid business I/O in readers,
and reject pending/unknown entity submissions in `presentation_ready`. Do not
serialize every field before applying this policy. Generated native adapters are
a separate generator gate; the explicit adapter is this component's boundary.

The immutable `AuthorizedResponseGraph` owns JSON values, paths, decisions and
Blob targets, with no entity, loader, original UserContext or save callback.
Unmodeled BlobRefs, nonfinite numbers and duplicate object members are rejected.
Input and output limits are 10,000 nodes, depth 64 and 8,388,608 UTF8 text bytes.
Credentials share the output budget. A document contributor can replace scalar
references while preserving the frozen entity/field/list/Blob surface. Blob
contributors see only visible targets; complete URLs and actual expiry are
validated and preserved.

## Existing tqd1 services

An existing `ContextBoundDocumentService` retains its open/accept behavior and
tqr1/tqd1 protocols. It opts in with `supports_view_preparation` and implements
`validate_prepared_document` to check current application document/purpose/field
authority using the authenticated snapshot. This check must not load another
business graph, save or issue another token. The default refuses this capability;
old open/accept methods retain their original behavior.

`open_document_for_view` calls that service once, consumes its returned token
with the current provider/principal and expected aggregate/purpose, checks the
Business ID and current application authority, then creates an opaque
`DocumentView`. Pass `ViewInput::Document(&source)` with governed options to
prepare. This adopts the body and existing credentials without calling open,
accept or issue again. An ordinary `ContextualDocument` or client JSON flag
cannot create this private provenance.

`accept_document_for_view` adopts successful acceptance in the same way. Use
`ViewOptions::governed().after_commit()`. Its `DocumentViewError` distinguishes
unchanged business-service errors from later presentation errors. A legacy error
is not relabeled as known rollback or commit. After successful acceptance, later
capture/preparation/reuse failures retain COMMITTED, including incorrect options
or a missing service. Re-read or retry presentation instead of replaying mutation.

Reuse reconsumes the existing token and rechecks current application authority,
principal, scope and deadline without issuing it again. Registered service and
provider identities must still match. DocumentContext is server-only; renderers
serialize the body and explicit credentials, never this context or its snapshot.

## Reuse, cache and transactions

Create one `ViewOptions` per presentation. Nested renderers use
`reuse_prepared_view(&prepared, &options)`, which checks private issuer,
presentation identity, option values, current visibility and resource authority,
then returns the same value without signing. Generic visibility is checked
before resource validation and again afterward. Fresh presentations use fresh
options and business data. PreparedView has no public constructor, Clone or
Deserialize, and its Debug output is redacted.

Cache defaults to `NoStore` (private, no-store). Positive private lifetimes are
capped by all resource constraints and absolute credential expiry. Renderers use
`cache_policy_at(actual_delivery_time)` so delay cannot extend a deadline.
Expired reuse fails without silent refresh.

Typed outer transactions and runtime-owned graph/ledger transactions fence
preparation from begin through physical completion. Successful commit/rollback
releases the fence before committed audit delivery. Completion errors and
cancelled begin/work/commit leave this context uncertain; use a fresh authorized
context and reload. QUERY is a caller-declared query result, never rollback proof.
These fences do not claim R5 PostgreSQL Blob/outer-transaction integration.

## Standalone consumers and remaining gates

`cargo run -p teaql-runtime --example view_preparation` runs the read-only terminal
consumer with a hidden getter and unchanged mutation state. It installs no Blob
or document implementation. The existing
`examples/order-management/rust-app-console/examples/context_bound_document_sqlite.rs`
consumer exercises actual SQLite open/accept adoption, a committed presentation
failure and presentation-only recovery.

The Blob contributor is an SPI here. Rust Blob storage, binding, full graph
snapshots, actual HTTP and the two-module combination belong to R5. Java governed
documents, selection/binding extensions, generated native adapters, framework
adapters, external-machine reads and registry/release gates remain separate.
No tqd2/tqs1 credential is introduced.
