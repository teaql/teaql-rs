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
