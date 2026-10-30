# teaql-blob-content-native

Optional native Rust parsing component for [Rust #312](https://github.com/teaql/teaql-rs/issues/312)
and [conformance #237](https://github.com/teaql/teaql-conformance/issues/237).
It implements the trusted `BlobContentVerifier` SPI without a JVM, business web
framework or storage SDK. Current-context upload authorization, filename/model
admission, catalog transitions and business bindings belong to the orchestrator.
Successful parsing alone does not advertise a complete BlobRuntime capability.

## Lifecycle and input

Construction is passive. The host explicitly calls `initialize` on a dedicated
absolute scratch root whose parent exists. Initialization refuses unrelated,
nonprivate or symlink paths and verifies Linux anonymous-file support. Readiness
does not repair or provision. A root marker is the only persistent scratch entry.

Each verification owns an O_TMPFILE inode opened relative to the retained private
directory. No payload name exists, including while another process is killed;
the kernel reclaims the inode after the last descriptor/mapping closes. It needs
no crash scratch sweep. Paths, raw content, hashes and credentials do not appear
in value/error formatting. Only the service UID can access the root and files.

Input streams write/hash in 64 KiB submissions under the smaller call/provider
byte bound. Once writes finish, the only writer transfers to a blocking parser
task and the immutable anonymous file is mapped read-only. Parsers receive slices
or read/seek views; no whole raw upload buffer is constructed. The one unsafe map
has a documented private-inode/no-concurrent-writer lifetime proof. Parser libraries
can allocate decoded pixels, fields, streams and other bounded format structures.

The semaphore is shared by clones. Capacity exhaustion returns
`BLOB_VERIFICATION_BUSY`. Input failure/size rejection/cancellation releases its
owned file. A parser already running retains its permit/file until the blocking
task actually ends, so cancellation cannot start unlimited replacement parsers.
There is no background parser or cleaner on construction.

## Actual format checks

The SPI accepts a canonical allowed extension and bytes; it accepts no declared
MIME/filename hint. MIME is returned only after the corresponding actual structure
passes. JPG/JPEG share image/jpeg. Empty input fails format detection even when
the byte policy permits empty content.

| Family | Checks in this component |
| --- | --- |
| JPEG/PNG | Actual decoder, positive dimensions, decoded-output budget, real scanlines and PNG CRC/end processing |
| MP3 | Complete MPEG Layer III frame bounds, bounded metadata and actual audio decoding/sample rate; ID3/APE tail recognition |
| MP4 | Box depth/count/remaining-byte checks including metadata/sample-entry containers, aggregate sample-table work budget, parsed video track/dimensions/samples and MP4 brands |
| DOC | CFB identity/stream integrity, WordDocument versus Excel discrimination; Word 97–2007 FIB, selected table, CLX/PlcPcd, character counts and actual bounded text spans |
| XLS | CFB/Workbook identity, BIFF8 records/encryption/dimensions/cell positions followed by Calamine worksheet parsing and cell/text limits |
| DOCX | Raw ZIP central-directory validation, unique names, actual bounded inflation/CRC, safe paths, namespace-aware XML, content-type identity and Word document/body structure |
| XLSX | Same package checks plus Excel identity, preflight dimensions/cell positions and actual Calamine worksheet parsing |
| PDF | Strict LoPDF syntax/xref parsing, catalog and cycle-checked page tree, bounded page-content decompression and parsed content operations/text operands |

ZIP validation runs before the library's name index can hide duplicate members.
Actual inflated bytes are bounded even when declared sizes lie. DTDs, unresolved
entities, invalid namespaces/structure, traversal, conflicting Office identity,
CRC errors and ambiguous duplicate declarations fail closed. Untrusted archive
members are never extracted or fetched as external URLs.

Defaults are 100 MiB input/expanded bytes, 10,000 package entries, 1,048,576 text
units/cells and 2 concurrent verifications. Byte arithmetic and cell spans are
checked. DOC uses UTF16 character positions; XML/cell counters use parsed text;
PDF additionally limits each eagerly decoded loader stream and the aggregate page
content. These are format budgets, not an OS address-space or CPU-time guarantee.
`NativeBlobContentVerifier` is the explicitly in-process inner implementation;
it has no forcible timeout. Public upload assembly should use
`ProcessBlobContentVerifier`, described below. Neither implementation claims malware
scanning, rendering or exhaustive validation of every Office/PDF feature.

## Resource-contained process verifier

`ProcessBlobContentVerifier` implements the same SPI with a separately installed
`teaql-blob-content-worker` ELF from the same package version. Configure an absolute
artifact path, its SHA256 and resource limits explicitly. Install it without any
write/setuid/setgid bits (for example mode 0500), owned by root or the service UID;
all path components must be real directories, without symlinks. Keep the installed
artifact and its directories under trusted host administration. Construction opens
nothing. Explicit initialization provisions only scratch and probes the worker;
readiness probes the existing installation without provisioning or repairing it.
Missing, changed, writable, mismatched or unsupported artifacts fail closed, with
no in-process fallback. Each invocation hashes a bounded regular ELF and executes
that exact opened inode through procfs, so atomic path replacement cannot substitute
an executable after validation. In-place modification by a trusted artifact owner
is outside this boundary; deployments must keep installed artifacts immutable.

The worker inherits only read-only anonymous input on fd 0, bounded stdout IPC on
fd 1 and discarded stderr on fd 2. All higher descriptors are marked CLOEXEC before
exec, including non-CLOEXEC files opened by the embedding host. No payload, locator,
principal, filename, hash or credentials enter argv/environment; only operation,
canonical extension and numeric parser budgets are arguments. Environment is exactly
LANG=C, LC_ALL=C, TZ=UTC and cwd is `/`. Parent and worker independently hash input;
the parent compares canonical MIME, byte count and SHA256 before exposing verified
facts. Replies are at most 4096 bytes, versioned, closed JSON with duplicate/unknown
fields, trailing data, incompatible versions and inconsistent exits rejected.
Worker stderr and panic/OOM details are never forwarded to callers or host logs.

Before exec the host creates a fresh session/process group and sets equal soft/hard
limits: default 1 GiB address space, 15 CPU seconds, zero core/file-write/process
budgets, 32 descriptors and 8 MiB stack. It sets no-new-privileges and SIGKILL on
parent-thread death with a parent identity race check. The worker disables dumps
and verifies those controls, equal nonzero real/effective/saved UID, and zero
permitted/effective/ambient capabilities. Readiness also checks the actual kernel
limits and exact supported format list. A privileged host fails readiness; configure
an ordinary service UID. Linux procfs, anonymous inodes and Linux 5.11+
close_range(CLOEXEC) support are required; unsupported controls fail readiness.

The default 30-second deadline covers artifact hashing, spawn, parsing and reply
collection; input streaming is subject to the caller's own request deadline and
byte limits. CPU/address-space limits are per worker, independently of parser
budgets. Cancellation, deadline or reply overflow kills the owned process group and
waits for the direct child. A clone-shared permit remains held through actual
reaping, including when the caller drops its future. Pure host diagnostics expose
bounded start/reap counts, live owned PIDs and a sanitized final outcome only.

This is process/resource containment of trusted pinned parsers, not a filesystem,
network or malicious-code sandbox. The worker shares the service UID's OS access;
untrusted executables must never be configured. No parser, listener, probe or
background task starts on construction. SQL orchestration remains separate.

## Dependencies and evidence

Direct parser versions are pinned in Cargo.toml and Cargo.lock. No registry source
is patched or vendored. A source investigation excluded rwml's whole-input-copy
path; Word validation reads CFB indexes/text spans directly using the published
MS-DOC layout. Calamine, LoPDF, image decoders, MP4 and Symphonia provide the other
actual parsers. Calamine's ZIP dependency enables Zopfli and requires newer crc32fast,
log and simd-adler32 patches; unrelated existing package identities are preserved.
Existing reference credential implementation/source remains unchanged.

Tests bundle the exact ten shared real files and all seven shared upload cases.
They cover type substitution, fabricated/truncated input, CRC/duplicate/traversal/
DTD/namespace/Office identity errors, byte/entry/text/cell/piece/expansion budgets,
source failure/capacity/cancellation, private readiness and actual process kills.
MP4 malformed metadata/work cases run in owned child processes with a test deadline;
that older test harness is distinct from the production process verifier.
Process-verifier tests additionally check all ten files/seven cases through the
installed worker, real CPU SIGKILL and memory-allocation SIGABRT, wall timeout,
cancellation/capacity recovery, bounded/forged replies, a non-CLOEXEC host descriptor,
minimal environment/read-only input, artifact replacement and actual host death.
An isolated owned subreaper verifies kernel parent-death SIGKILL and reaps only its
exact adopted child. Test-only fault modes live exclusively in a separate binary
behind `process-fixtures`; do not install that feature's fixture executable.

Full SQL upload orchestration, native save/journal/read integration, production S3,
governed document/selection combinations and original R5–R8 exits remain in progress.

Transport/verifier values come directly from the lightweight teaql-blob-contracts
crate; teaql-blob-reference re-exports the same types for compatibility. The
provider production graph stays independent of the host runtime-integration feature.
