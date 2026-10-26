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
The current parser task runs in-process and has no forcible timeout. An isolated
worker with process memory/CPU limits remains required before full public Blob
upload assembly; this inner component does not claim process isolation, malware
scanning, rendering or exhaustive validation of every Office/PDF feature.

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
that test harness is not a production isolation capability.

Full SQL upload orchestration, native save/journal/read integration, production S3,
governed document/selection combinations and original R5–R8 exits remain in progress.
