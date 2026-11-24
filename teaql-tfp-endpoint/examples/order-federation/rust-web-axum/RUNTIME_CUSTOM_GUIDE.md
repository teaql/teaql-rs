
# TeaQL Axum Runtime & Framework API Reference

> [!WARNING]
> **DO NOT GUESS FRAMEWORK APIS**
> Do not guess how to use `TeaContext`, `ContextProvider`, `UserContext`, `SmartList`, `WebResponse`, schema setup, or save pipelines.

For exact runtime examples, execute:

```bash
cargo teaql --input models/order-federation-service.xml rust-assist-runtime-custom
```

Axum integration comes from `teaql-web-integration-axum`, which provides:

- `WebResponse<T>` for Java-compatible JSON responses
- `AxumTeaError` for handler error responses
- `ContextProvider` for app state
- `TeaContext` as an Axum extractor for `UserContext`