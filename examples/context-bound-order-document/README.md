# Context-Bound Order Document POC

This executable test is the first backend proof of TeaQL's governed document
round-trip design. It uses an in-memory Order store only to keep the proof
deterministic; the runtime API and `ContextBoundDocumentService` SPI are
provider-neutral.

Run it with:

```bash
cargo test -p teaql-examples --test context_bound_order_document
```

The three tests prove:

- Alice and Bob receive different projections and authenticated `tqd1` tokens;
- Bob cannot submit Alice's document;
- hidden rows survive an update and omission never implies deletion;
- removal requires an explicit `removedRefs` intent on a complete, writable
  relation;
- read-only and unknown fields cannot be mutated;
- validation failure does not consume the document and the corrected request
  can reuse it;
- successful acceptance advances the aggregate revision and returns a fresh
  snapshot;
- stale aggregate and child versions fail with `DOCUMENT_REVISION_CONFLICT`;
- references from another Order cannot be substituted;
- current authorization is evaluated again when the document returns;
- a restart using the same key ring accepts the token, while another
  environment rejects it.

This POC intentionally does not claim generated Order adapters, a database
transaction implementation, or parity in the other six runtimes. Those are
separate evidence gates.
