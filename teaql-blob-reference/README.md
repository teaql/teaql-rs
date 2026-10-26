# teaql-blob-reference

This optional crate currently implements the R5 policy/catalog component.
BlobRuntime/BlobReferenceModule assembly, upload streaming/format verification,
File/S3 storage, read projection, durable command recovery, native graph/ledger
and outer completion, and governed selection remain in progress. Runtime saves
do not automatically call this catalog yet. No complete Blob capability is
advertised by installing a partial implementation.

Production dependencies are core/data-service contracts, Serde and SHA256.
Runtime/PostgreSQL/Tokio dependencies are for tests. A base runtime does not
install this implementation transitively.

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
