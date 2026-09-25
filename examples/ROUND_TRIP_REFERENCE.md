# Round-Trip Reference runtime example

`tests/round_trip_reference_runtime.rs` proves the framework-neutral runtime
serialization boundary. It is intended to be called by Axum, Actix, Rocket,
or another boundary adapter; none of the reference cryptography belongs to a
specific Web framework.

The example covers governed issue/resolve, opaque context binding, transfer
and tamper rejection, current/previous key rotation, and the explicit
development-only raw diagnostic representation.

The core contract receives the complete `UserContext`; it does not define what
the context binding means. The runtime customization supplies opaque binding
bytes. Successfully resolving a reference only restores `(type, id, version)`:
the application must still re-run its current authorization and policy checks.

```bash
cargo test -p teaql-examples --test round_trip_reference_runtime
```
