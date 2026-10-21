# TeaQL controlled HTTP provider

`ReqwestHttpRequestService` implements the provider-neutral
`teaql_runtime::HttpRequestService`. Trusted startup code registers named
`HttpServiceProfile`s, then installs the shared service on `UserContext`.
Application calls specify only a profile alias and relative path, together
with comment and purpose. Redirects and implicit retries are disabled.

This provider is separate from native SDKs such as S3: it never wraps an SDK's
already signed request or adds a second SDK retry loop. Run its local fixture
with `cargo test -p teaql-http-reqwest` and the native example with
`bash examples/http-resilience/verify.sh`.

Trusted startup can call `profile.trusted_root_pem(&pem)?` or
`profile.trusted_roots_pem(&[&old_pem, &new_pem])?` before registering the service.
The explicit one-to-four-root set replaces system trust only for that HTTPS
profile, without disabling peer or hostname verification. Each PEM contains one
certificate and is limited to 64 KiB; empty, duplicate, bundled, malformed,
oversized or plaintext-profile inputs fail before use. Other profiles keep
ordinary system trust. Clone/rebuild profiles to roll from old-only through
old+new to new-only trust; existing profiles and clients remain unchanged.

`tests/http_profile_trust.rs` proves these boundaries with two live loopback
HTTPS peers and cumulative rotation rounds. It uses local OpenSSL, automatically
removes generated private keys and requires neither Vault nor external services.
The standard HTTP example verifier runs this native test along with the other
HTTP tests.
