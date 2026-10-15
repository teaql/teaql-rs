# Controlled HTTP from a runtime Context

Run `bash examples/http-resilience/verify.sh`. The example uses a local
loopback-only server, a trusted startup profile, and a Context-owned
`ReqwestHttpRequestService`. Application code supplies only the alias,
relative path, comment, and purpose. The verifier runs the executable twice;
the provider's separate test suite covers failure and cancellation paths.
