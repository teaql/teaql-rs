
# TeaQL Tool API Reference

> [!WARNING]
> **DO NOT GUESS TOOL APIS**
> Do not guess how to use `context.http()` or other built-in tool integrations.

> [!IMPORTANT]
> **USE TEAQL'S OWN TOOLING FIRST**
> Always use `cargo teaql` to retrieve assist content. Never bypass the CLI or invoke the generator endpoint directly.

To get exact TeaQL Tool API usage, execute:

```bash
cargo teaql --input models/order-federation-service.xml rust-assist-tool-api/[module]
```

| module | T:: Facade | Description |
|--------|------------|-------------|
| http   | `context.http()` | HTTP client for external service calls |

For domain object assist APIs, use `rust-assist-query/[object]`, `rust-assist-create/[object]`, `rust-assist-update/[object]`, or `rust-assist-delete/[object]`.