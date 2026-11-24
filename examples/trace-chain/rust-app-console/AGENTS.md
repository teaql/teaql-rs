<!-- DISCARD_BLOCK: phase_modeling -->

# TeaQL Rust Agent Instructions

## TeaQL workspace rules

- Work in application-owned files. Keep the generated domain library read-only.
- Discover API names and signatures through model-aware Assist only.
  Do not read or search generated library source for API discovery. Use the
  original evaluated KSML file or directory and its exact entity and field names.
- Ask for the current entity and action first. Request field-level query help
  only for the fields you use. If Assist lacks the operation, report
  `MISSING_ASSIST` with the entity, action, and relevant field or diagnostic.

```text
cargo teaql --input [model] rust-assist-[action]/[entity]
cargo teaql --input [model] rust-assist-query/[entity].[field]
```

Replace the bracketed values with the actual model path, action, entity, and
field. Actions include `query`, `list-page`, `create`, `update`, `delete`, and
`expression`. Validate API usage by compiling and testing the application.

- Use the trusted `UserContext` as the only runtime argument for generated query
  execution and saves. Resolve tenant, actor, authorization, and provider
  resources from trusted application setup.
- Keep queries bounded and provide a non-empty purpose and comment. Supply a
  non-empty audit reason for writes through the API shown by Assist.

> [!WARNING]
> **IGNORE GENERIC ORM EXPERIENCE**
>
> Do **not** use pre-trained habits from data-access frameworks, ORMs, or database integration libraries.
>
> Do **not** use SeaORM, Diesel, SQLx, rbatis, or similar frameworks.
>
> Do **not** write raw SQL, DAOs, Repository implementations, or custom persistence layers.
>
> Do **not** guess TeaQL method names.

## Model entities

| entity-name | display-name |
|-------------|--------------|
| platform | Platform |
| customer_order | Customer Order |
| order_item | Order Item |
| payment | Payment |
| payment_attempt | Payment Attempt |
| shipment | Shipment |


Create each application-owned source file once. After its first compile
attempt, use the smallest localized patch for each exact diagnostic and
preserve unrelated compiling code. Do not rewrite a complete file as a repair
strategy. Before replacing more than 25% of an existing application file, stop
and report `LARGE_REWRITE_REQUEST` with the file, diagnostic, reason, and
estimated scope. Initial creation and model-driven regeneration are exempt.

## Application entry and context

Start in `src/main.rs`. With a configured provider, use the context from
`service_runtime_from_env()`. For module-only setups, build the context from
`module_with_behaviors_and_checkers().into_context()`.

## Additional References

Read these only when the task requires them:

* **`RUNTIME_CUSTOM_GUIDE.md`**
  Runtime setup, framework APIs (UserContext, SmartList, WebResponse, etc.), and debugging.

* **`TOOL_API_GUIDE.md`**
  Built-in tool integrations (HTTP client, etc.).

## Optional standard telemetry

See [STANDARD_TELEMETRY.md](STANDARD_TELEMETRY.md) for the selected runtime's optional module, trusted activation variables and safe startup diagnostics.
