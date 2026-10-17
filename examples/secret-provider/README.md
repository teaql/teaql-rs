# Context-owned Secret Provider example

Run `bash examples/secret-provider/verify.sh`. The first runtime-owned executable
binds a symbolic secret reference to an owner-only local file at startup,
resolves it through `UserContext` with a comment, rotates the file, and proves
the next resolution sees the new value.

The second executable installs a fixed Vault KV v2 path and a separate bootstrap
token provider, then uses the real HTTP adapter against a loopback fixture. It
checks the path, token header, response version, and a stalled-response timeout.
The verifier runs both examples twice and rejects plaintext canaries in output.
This does not require or claim a production Vault deployment.

Private-CA rollover is explicit: `trusted_roots_pem(&[old_pem, new_pem])`
temporarily trusts both roots for this named HTTPS profile, replacing system
roots rather than expanding process-wide trust. A later profile with only
`trusted_root_pem(new_pem)` retires the old CA for new requests. The TLS
example checks reads through both CA-signed server certificates during
overlap and rejects the old certificate after retirement. Each slice element
contains one PEM certificate; the bounded profile accepts at most four roots.
Replacing the service requires `&mut UserContext`, so safe Rust cannot replace
it while an async secret resolution borrows that same Context. Replacement is
performed after the call completes; separate Contexts need coordinated rollout.
Production certificate rollout and requests in other instances require
separate draining and verification.
The controlled two-instance fixture keeps old- and new-CA servers online
together, advances independent Contexts through mixed and overlap profiles,
rejects an old endpoint after trust retirement, then restores the old
instance by routing it to the new endpoint. It checks exact server-side HTTP
request counts; it is not a separate-host production rollout.
The example also accepts an old-CA Vault read but deliberately never returns
its response. The read ends within its 900 ms whole-call budget as
`Unavailable`, without a second HTTP request; only after the borrow ends is
the Context switched to new-only trust. This is a bounded-drain test, not
proof that changing trust immediately cancels in-flight requests.
