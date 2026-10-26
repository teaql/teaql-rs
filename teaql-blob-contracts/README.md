# teaql-blob-contracts

Lightweight Blob values, sanitized errors, scope/field targets, cache/disposition,
immutable store/range/signed-URL contracts and actual content-verifier SPI. This
crate has no runtime, PostgreSQL provider, web framework, storage SDK or parser.
It performs no provisioning, background work or contextual authorization.

The original teaql-blob-reference public paths re-export these same types for source
compatibility. The catalog/policies and actual UserContext upload implementation
remain in that optional implementation crate. File/native parser providers depend
only on contracts, independently of the host runtime-integration feature. Existing
policy JSON, stable Blob Key JSON and tqr1/tqd1 protocols are unchanged.

VerifiedBlobContent is trusted server output with closed validated facts and no
client deserializer. BlobStore is trusted I/O after orchestrator authorization;
knowing a locator, upload ID or stable Key grants no read/write authority. Future
runtime participants can share these values without a dependency on the concrete
Blob implementation or a dependency cycle.
