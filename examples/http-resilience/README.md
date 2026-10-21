# Controlled HTTP from a runtime Context

Run `bash examples/http-resilience/verify.sh`. The example uses a local
loopback-only server, a trusted startup profile, and a Context-owned
`ReqwestHttpRequestService`. Application code supplies only the alias,
relative path, comment, and purpose. The verifier runs the executable twice;
the provider's separate test suite covers failure and cancellation paths.
Each execution performs two rounds against one live fixture with one Context
and service; cumulative request/trace counts increase from two to four.
The verifier retains those safe same-fixture markers in its console output and
rejects missing rounds or private request canaries. No fixture reset is used.
