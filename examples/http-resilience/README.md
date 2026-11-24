# Synchronize supplier order status

The [supplier model](model.xml) defines the supplier and orders awaiting
confirmation. The generated workspace installs a named HTTP profile on
the Context and reconciles orders against a loopback supplier. A direct response
and one retry update their own orders. Retry exhaustion, cancellation and a
response for another order leave the local versions unchanged.

```bash
bash examples/http-resilience/verify-sync.sh
```

## Original HTTP calibration

Run `bash examples/http-resilience/verify.sh`. The example uses a local
loopback-only server, a trusted startup profile, and a Context-owned
`ReqwestHttpRequestService`. Application code supplies only the alias,
relative path, comment, and purpose. The verifier runs the executable twice;
the provider's separate test suite covers failure and cancellation paths.
Each execution performs two rounds against one live fixture with one Context
and service; cumulative request/trace counts increase from two to four.
The verifier retains those safe same-fixture markers in its console output and
rejects missing rounds or private request canaries. No fixture reset is used.
