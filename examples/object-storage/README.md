# Context owned object storage example

Tracking: [Rust #249](https://github.com/teaql/teaql-rs/issues/249),
[shared contract #122](https://github.com/teaql/teaql-conformance/issues/122).

The example uses this checkout's core, runtime and optional S3 provider through
workspace path dependencies. No generated domain library is required.

```rust,no_run
use std::sync::Arc;
use teaql_runtime::{UserContext, object_storage::*};
use teaql_object_storage_s3::S3ObjectStorage;

// sdk_client is configured once at trusted startup: endpoint, region,
// credentials provider/refresh, request timeouts and path-style routing.
let provider = Arc::new(S3ObjectStorage::new(sdk_client, "attachments")?);
let context = UserContext::new().with_object_storage(provider);
let storage = context.object_storage("store payment receipt attachment")?;
let key = ObjectKey::new("receipts/opaque-reference.pdf")?;
storage.put(&key, content_stream, &PutOptions::new(size, WriteMode::CreateOnly)?).await?;
let link = storage.get_read_url(&key, &ReadUrlOptions::default()).await?;
// Send link.as_str() to the authorized caller; never log it.
```

SDK clients clone cheaply and share pools. Provider selection belongs to trusted
context assembly, not request JSON. The SDK credentials provider handles session
credentials/refresh. The sample uses explicit test-only credentials from env.
Default operation diagnostics and runtime telemetry are automatic; a typed
`ObjectStorageObserver` can receive safe operation events. It never receives
keys, body bytes, custom metadata, signed URLs or raw provider failures.

The six operations are put, get, head, exists, delete and get_read_url. Keys are
durable; URLs are reusable temporary bearer links (default five minutes). The
expiry deadline is an upper bound subject to credential lifetime/provider policy.
URL issuance does not check existence or grant domain permission automatically.

Uploads use bounded-memory temporary-file staging for exact length checks and
retryable single PUTs, capped at 5 GiB. This consumes disk and is not concurrent
upload streaming or multipart. Downloads stream directly; drop releases them.
`get_bytes` requires an explicit size limit. Create-only is atomic; replacement
is explicit. Object-store effects are independent of SQL transactions.

## Verify with a dedicated local S3 service

Set `TEAQL_S3_ENDPOINT`, `TEAQL_S3_BUCKET`, `TEAQL_S3_ACCESS_KEY` and
`TEAQL_S3_SECRET_KEY` to a test-only service/bucket, then run:

```bash
bash examples/object-storage/verify.sh
bash examples/object-storage/verify.sh
```

The example provisions its dedicated test bucket if missing (the provider never
does). Unique test keys are deleted after successful assertions. Failure retains
the unique keys for diagnosis; no bucket or prefix is recursively deleted.
The script requires configuration and runs offline contract/error tests plus
real CRUD, metadata, concurrent create, bounded reads, signed URL download and
expiry, replacement, missing-bucket and repeated-delete checks.

The SDK is pinned in the provider manifest and lockfile. Local S3-compatible
evidence does not certify AWS-hosted S3 or every compatible service.
