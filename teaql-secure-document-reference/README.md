# Optional secure document reference crate

Tracking [Rust #312](https://github.com/teaql/teaql-rs/issues/312) and
[conformance #237](https://github.com/teaql/teaql-conformance/issues/237).

`teaql-runtime` retains only principal/identity/scope and snapshot types,
service/provider SPI, wire codec and delegating UserContext APIs. This optional
crate owns the existing ContextBoundReferenceRuntime, key/configuration/policy
and tqr1/tqd1 cryptography. Import those concrete types from this crate. The
runtime does not depend on this crate or reexport its implementation; no cycle
or default feature forces installation.

Use `SecureDocumentReferenceModule::new(provider)` and optional
`.with_document_service(application_service)`, then `.install_into(&mut ctx)`.
The module inserts only typed shared service resources and never calls
RuntimeModule.apply_to, changes business registries, loads documents, issues
tokens, migrates a schema or starts jobs. Repeated installation returns a
configuration error. Existing trusted-context facade setters remain available
but reject overwriting an installed service. Principals stay request-local.

After assembly, `validate_endpoint(&ctx, needs_document_service)` checks governed
endpoint requirements. Runtime reference and open/accept entry points still
reject missing protection. Base/Blob-only programs do not need document keys
or this crate. Ordinary mode does not use the raw-ID diagnostic switch.

The original crypto and document codec bodies and tests move together. Existing
tqr1/tqd1 bytes, derivation, keys and current authorization semantics remain.
The old entity_reference implementation has separate remaining users and crypto
dependencies and is preserved. New tqd2/tqs1 and Blob preparation/selection are
later work; extraction does not assert those capabilities or registry release.
