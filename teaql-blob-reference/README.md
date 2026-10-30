# teaql-blob-reference

This optional crate implements policies, the PostgreSQL catalog and, with the
explicit `runtime-integration` feature, actual current-UserContext upload services
and a transaction-bound mutation participant. Audited graph, typed ledger and
native outer/raw saves call the participant when it is explicitly installed;
missing participation still fails before business DML. The optional
BlobResponseContributor joins current field bindings with the catalog in one
PostgreSQL snapshot, checks current read authority, and asks a signed-read
BlobStore for complete URLs only for visible, loaded fields. Hosts explicitly
attach it to ViewPreparationService; prepared views recheck authority and the
binding without signing again. Full `blob.v1` readiness, production S3,
external response bridges, durable command recovery and governed selection
remain in progress. A standalone upload service does not advertise a complete
Blob capability.

The trusted asynchronous BlobStore SPI now defines immutable streaming, range,
exact-version delete/seal and complete signed-read values. The optional sibling
teaql-blob-store-file implements Linux file transport and a feature-gated HTTP
gateway; it does not install a complete Blob capability or verify actual formats.
The optional sibling teaql-blob-content-native now implements actual byte/format
parsing behind BlobContentVerifier. VerifiedBlobContent is not deserializable client
input. Its parser/anonymous-scratch budgets and optional pinned resource-contained worker
are documented separately; full SQL upload orchestration remains in progress.

Default production dependencies are lightweight Blob/core/data-service contracts,
Serde and SHA256. Transport/verifier values now live in `teaql-blob-contracts` and
remain re-exported from this crate with the same public type identities. File and
native parser implementations depend directly on contracts, so enabling the host
runtime feature does not link the runtime into a parser worker. The optional
runtime-integration feature adds actual TeaQL UserContext, Tokio deadlines, bounded
stream adapters and OS randomness. PostgreSQL/provider implementations remain test
or injected host dependencies. A base runtime does not install this implementation
transitively; there is no reverse Web-framework dependency or crate cycle.

## Policies and wire compatibility

BlobPolicies combines deployment limits, defaults, named profiles and canonical
field mappings. Formats intersect the model/deployment/profile sets. Invalid
sizes/durations/cache policies fail; inline MIME support is checked against the
actual model field. Current read TTL takes the minimum profile/request/deployment/
provider/document limit. Context-checked reads require an explicit capability.
Accepted retention preserves the maximum historical promise.

BlobPolicySnapshot uses Java's existing closed catalog JSON schema v1. Both the
bounded decoder and direct Serde decoder validate it, including rejection of
unknown/duplicate properties and duplicate extensions. The shared vector is
emitted by actual existing Java JARs and round-trips to identical Rust bytes.
This storage format does not change tqr1/tqd1 credentials.

## Actual SQL ownership

The framework-only data-service infrastructure_sql SPI exposes the native
executor's SQL resource and physical transaction identity. PostgreSQL implements
it; other providers default to unsupported. SqlDataService wrappers forward the
actual native handle. Separately constructed transports have different resource
identities even when given the same Pool; clone the configured transport to share
its resource. Each begun transaction has a fresh identity and read-only outcome
witness. An identity cannot be deserialized or used to change completion state.

PostgreSQL retains its connection through acknowledged physical completion.
SQL/logical catalog failures mark rollback-only; catching an error and committing
cannot turn an aborted transaction into apparent success. Cancelled BEGIN/work/
COMMIT or failed completion discards the connection instead of pooling it dirty.
The server may commit before an acknowledgement is lost: UNKNOWN is preserved.

## Trusted catalog orchestration

Construction is passive. Migrate is explicit and fixed; its SQL bytes match Java.
Version/digest, required columns and key constraints are checked. Readiness never
repairs a partial or tampered schema. There is no generic DDL diff.

Begin/verified-completion are low-level trusted upload-orchestrator APIs. The
orchestrator must generate identities/locators and the canonical request digest,
authorize the current host, enforce current hard limits, stream immutable content
and verify actual format/bytes. A client completion flag is insufficient. Current
tests exercise trusted verified-content metadata, not physical upload/verifier
implementation by this crate.

`find_upload` is scope-qualified; `find_upload_request` compares the trusted canonical
request digest before returning an old session. `lock_upload` returns an opaque,
non-cloneable BlobUploadLease owning the actual physical transaction and row lock.
It exposes only a read-only catalog record, not SQL or a mutable completion owner.
Hold this lease across the actual store write/read and current-context authorization
rechecks. Its async record_write/complete_verified methods consume it and return a
new lease only on success; failure rolls back and cancellation drops the owned
transaction/connection. A caller cannot catch a failed update and commit a retained
handle. Commit and rollback consume the lease too; errors preserve known completion
or UNKNOWN, never infer rollback from a missing COMMIT acknowledgement.

record_write journals exact immutable location/version/size/SHA without setting MIME,
completed_at or uploaded_unbound. Replays must match; conflicts cannot replace the
original generation. Confirmation compares prior journaled version/size/hash before
promotion. This remains a trusted low-level API: direct completion without an earlier
journal is reserved for bytes actually verified by the orchestrator (for example a
future direct-upload provider), not a client completion flag. Unverified partial or
corrupt journal tuples fail closed instead of being repaired through completion.

Owned upload transactions require PostgreSQL 14+ on a server supporting socket
connection polling (tested here on Linux). validate_upload_ready checks existing schema, server version and setting
availability read-only. Each lease sets client_connection_check_interval=100ms
transaction-locally, so a cancelled query blocked on a lock can notice the closed
client socket before the lock holder releases it. This neither changes global/session
configuration nor imposes a statement timeout. Tests show the setting restores on
commit/rollback and the same pooled backend can be reused. Kernel detection of a
network partition still depends on deployment TCP keepalive/timeouts; this setting
is not a fixed end-to-end cancellation deadline. No detached query-cancellation task
or hardcoded TLS connector is created.

Prepare-bindings consumes an already approved batch with final allocated IDs
and a borrowed physical SQL handle. It checks resource/owner before I/O, locks
targets/objects in stable order, checks prior Key/version and current policy,
lifetime and net sharing. PreparedBlobBindings freezes private provenance for
that transaction; apply consumes it without committing. Logical errors poison
the borrowed owner too. Delayed apply starts minimum retention at actual boundAt
and checks arithmetic before binding writes.

Replacement/clear keeps versioned tombstones. Last-reference release and accepted
retention/single-binding promises remain durable. Collection locks and rechecks
references atomically; active bindings prevent collection and deleting objects
cannot be rebound. Acknowledgement is only for trusted maintenance after successful
version-conditional physical deletion; the catalog does not delete content.
Corrupt completed version/digest/retention facts fail closed.

Owned failures carry a sanitized code and physical completion when known. Borrowed
failures require the owner to finish. UNKNOWN never proves rollback or authorizes
blind mutation replay. Full runtime journal/command recovery remains R5 work.

## Actual upload service

Enable runtime-integration and construct BlobUploadService with an existing catalog,
injected BlobStore/BlobContentVerifier, policies, a required BlobUploadAuthorization
and a clock. Construction is passive. Provision storage/parser roots and migrate
schema explicitly before validate_ready checks existing infrastructure and current
model descriptors. This feature provides server-stream uploads; direct mode fails
explicitly until a complete direct-target/production provider is installed in a
later implementation phase. It never falls back to client MIME or string fields.

Every operation receives the actual UserContext. The host authorization policy must
resolve its verified actor/scope from that context and independently approve field
writes and the Blob action; there is no allow-all/default OS-user authority. Model
fields must be canonical BlobRef properties with consistent BlobFieldSpec metadata.
Checks repeat around I/O and before SQL commit. An identity/scope change within an
operation, narrowed current model, hard deployment/provider size limit, expired
session or rejected final verified facts cannot promote content.

begin normalizes a basename, bounds filename/MIME hints and checks canonical
length-delimited request replay. IDs use 24 OS-random bytes; callers cannot choose a
locator or version. write owns the catalog row lock across actual immutable storage
I/O and journals exact generation/size/SHA only after current authorization and time
checks. complete requires a successful server-stream journal, stats the immutable
generation, opens the full exact range and invokes the actual verifier. Returned
MIME/size/hash must match canonical extension/storage facts before uploaded_unbound.
Repeated complete checks current authority, retention and immutable facts without
parsing again. Declared MIME never supplies the verified MIME.

Storage/verification/lock waits use the session or unbound-retention deadline; stream
polls also check the clock. Timeout/cancellation releases the owned SQL transaction
and storage stream; the process verifier retains its own capacity until actual
reaping. Rejection after a physical write can leave immutable unverified bytes,
which are reserved for retry/expiry collection, never automatic binding. Starting
COMMIT requires a live authorized session; a lost acknowledgement keeps UNKNOWN
and does not authorize blind replay. Host request/TCP deadlines still govern actual
COMMIT completion and active bound-object reads without a retention deadline.

Source integration tests use actual UserContext metadata/typed principals, dedicated
PostgreSQL, FileBlobStore and a separately built SHA-pinned production worker. They
cover all ten formats/seven cases, dual denials, current model/hard limits, actor/
scope/time changes, bad provider facts, pending-source deadlines/cancellation,
unverified state, SQL identity allocation/binding/unbinding and exact-version
collection. Native graph/ledger/outer save integration and a separate actual
PostgreSQL plus file-gateway view test cover transaction binding and signed
HTTP GET/Range, hidden fields, revocation, unbinding and governed expiry.
Full module/four-combination acceptance remains required before the original R5 exit.

## Gates

Unit tests consume all 13 shared policy/retention cases and the actual Java
21-field vector. The explicit live-postgres test feature requires a dedicated
host runner, credentials, run label, actual fixture and exact cleanup manifest;
missing configuration fails and no live test silently skips. Actual native SQL +
catalog txids, independent connections, rollback/rollback-only, owner rejection,
tombstones, shared retention, net swaps, real GC lock waiting and test-only wire
faults for BEGIN cancellation/COMMIT-ack loss are exercised.

Complete module/four-combination acceptance, generated consumers, framework
adapters and registry/release gates remain separate.

The embedding host must also bound admission/lookups before a session deadline is
known, and acknowledged COMMIT completion. A session timer is not a universal
network or database cancellation deadline.
