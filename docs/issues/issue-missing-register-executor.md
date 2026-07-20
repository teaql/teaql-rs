# Bug: Generated context helper `service_runtime_from_env` missing `register_executor` call

## Description
When running entity saves via `AuditedSaveExt::save(&ctx)`, the runtime requires a type-erased helper `DynGraphSaver` to be registered.

However, the template used to generate the context builder (e.g. `service_runtime_from_pool` in `runtime.rs`) only inserts the executor as a generic resource:

```rust
context.insert_resource(ServiceRuntimeExecutor::new(mutation_executor));
```

Because it does not call `context.register_executor(executor)` explicitly, the `DynGraphSaver` trait object is never bound. As a result, calling `.save()` on any audited entity fails at runtime with:
`graph write error: no DynGraphSaver registered — did you call register_executor()?`

## Expected Behavior
The context initialization helper should register the executor correctly so that graph saves can be performed immediately without manual registration workarounds.

## Suggested Solution
Update the Rust code generator template for `runtime.rs` to call `register_executor` on the context:

```rust
let executor = ServiceRuntimeExecutor::new(mutation_executor);
context.register_executor(executor.clone());
context.insert_resource(executor);
```
