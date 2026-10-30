# teaql-blob-store-file

Optional Linux file transport for the independent `teaql-blob-reference` module.
Tracked by [Rust #312](https://github.com/teaql/teaql-rs/issues/312) and
[conformance #237](https://github.com/teaql/teaql-conformance/issues/237).

This provider supplies actual bounded streaming storage and downloads. Upload
authorization, format/container verification, catalog lifecycle, business bindings
and current read authorization belong to the Blob orchestrator above this trusted
transport. They are not implied by storing bytes successfully. Complete Rust Blob
assembly and R5 acceptance remain in progress.

## Explicit filesystem lifecycle

`FileBlobStore::new(absolute_root, maximum_write_bytes, concurrent_writes)` is
passive. The parent exists already; `initialize().await` explicitly creates the
final dedicated root or reopens its recognized format. `validate_ready().await`
only checks. Roots/directories must belong to the service UID with no group/other
permissions. Files are private regular single-link files. Every path component
is opened without following symlinks; child operations use retained directory
descriptors and refuse replaced children. Unrelated roots remain untouched.

The Rust format marker is `teaql.rust.file.blob-store/v1`. It differs from Java's
file provider: their private layouts and OS lock mechanisms do not constitute a
shared filesystem protocol. Use dedicated roots. Java/Rust Blob semantic contracts
and catalog metadata remain shared; production S3 interoperability is separate.

Locations derive from server-generated upload identities. A bounded write streams
in 64 KiB filesystem submissions, computes SHA256 and stores an immutable random
generation, size and digest header. Same-byte retries return the original version;
different bytes conflict. Publication uses a no-overwrite atomic rename and fsync.
There is no overwrite fallback or unbounded content buffer. Empty content can be
stored; the independent verifier enforces the field's format/allowEmpty policy.

Clones share the configured write semaphore. Independent processes coordinate
publication, exact-version deletion and sealing through a fixed set of kernel
file locks. Lock waits are cancellable. An opened range owns its file descriptor;
deletion and a later generation cannot change the bytes already opened. Every
stream chunk remains bounded; the caller owns/drops its input and output streams.

After a committed catalog deleting claim, `seal_for_deletion` permanently closes
that upload namespace under the same lock as publication. The seal is atomically
published and durable before absence can acknowledge deletion. It rejects a
different expected generation, fences writes already streaming, and survives
restart. `delete_if_version` independently enforces the exact current generation.
Catalog bindings/claims must still be checked by the orchestrator.

Scratch files carry OS leases held through publication. Normal errors/cancellation
unlink only their own scratch. The host explicitly calls `cleanup_staging(now,
minimum_age)` for crash recovery; active leases in other processes are skipped.
Only recognized scratch names are eligible. No background cleaner is installed.
The host controls scheduling/capacity and retains the small seal/lock records.

## Optional HTTP gateway

Enable `http-gateway` for `FileBlobHttpGateway`. The default file provider has no
HTTP/crypto implementation and explicitly rejects signed reads. Core/base runtime
does not install either provider.

Configure an absolute public HTTP(S) endpoint, an active key ID, a retained key ring,
a trusted clock and maximum whole-second TTL. Keys are separate from document
reference keys, with 32–128 bytes each. Construction opens no listener. The host
binds a Tokio TcpListener, runs `Arc<...>::serve(listener, stop_future)`, and awaits
its completion to close the listener and its bounded connection tasks. TLS/public
reachability are host deployment concerns. Readiness/signing require an active
listener and a ready file root. No Spring/Axum business route is required.

`sign_read_batch` returns complete absolute URLs with actual UTC millisecond
expiries capped by the provider limit. Signed claims bind origin, generation,
content type, filename, disposition, private cache policy and lifetime using a
versioned, domain-separated HMAC envelope (`rbf1`, private to this gateway).
Retaining verification keys preserves older URLs during key rotation. Values and
errors do not print locators, content versions, key bytes or URLs.

Actual GET/HEAD supports a single closed, open-ended or suffix byte range, exact
Content-Length/Content-Range, MIME, RFC5987 filename, nosniff and private cache
headers capped again at send time. Expiry is rechecked after storage lock waits.
Unsupported/multiple ranges fail with 416; tampered/expired credentials reject
before content access. An old URL cannot read a replacement generation. URL expiry
does not delete content. These are transferable bearer URLs; context-checked reads
and direct uploads are explicitly unsupported.

## Verification

Tests use the exact ten shared real-file fixtures bundled under `tests/fixtures`.
They verify bytes/digests/ranges, retries/conflicts, wrong-version deletion, opened
inodes, private namespaces, actual subprocess locks/cancellation, active scratch
leases, process kills/recovery and durable publication. Gateway tests use actual
loopback HTTP and independent curl, wall-clock expiry/reissue, generation fences,
key rotation, origin/tamper/range rejection and cache/expiry after a real lock wait.
No database or parser success is inferred from these transport tests.

Transport/verifier values come directly from the lightweight teaql-blob-contracts
crate; teaql-blob-reference re-exports the same types for compatibility. The
provider production graph stays independent of the host runtime-integration feature.
