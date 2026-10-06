<!-- DISCARD_BLOCK: phase_modeling -->

# TeaQL Rust Agent Instructions

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

## How to Write Domain Code

To get the exact API usage and query examples for the entity you are working on, execute the following command:

```bash
cargo teaql --input models/trace-chain-service.xml rust-assist-[action]/[entity-name]
```

> `models/trace-chain-service.xml` is the default model path. If the model file is located elsewhere, adjust the `--input` path to match the actual file location in this project.

Replace `[action]` with one of the following:

| action | when-to-use |
|--------|-------------|
| query | Read/find records from the database using Q:: |
| create | Insert a new record into the database |
| update | Modify and save an existing record |
| delete | Remove or soft-delete a record |
| expression | Safely extract nested relation values using E:: |
| list-page | Implement a paginated query returning SmartList |
| debug | View instructions for enabling SQL logging and debugging |

Replace `[entity-name]` with the exact entity-name from the table below:

| entity-name | display-name |
|-------------|--------------|
| platform | Platform |
| customer_order | Customer Order |
| order_item | Order Item |
| payment | Payment |
| payment_attempt | Payment Attempt |
| shipment | Shipment |


Once the command succeeds, read its output. Use the printed code as a template to write your logic.

If the command cannot be executed, stop and report the missing context. Do not invent APIs.

Do not inspect, grep, or search generated `lib/src` files to discover APIs. If
the current entity/action Assist and required field-specific Assist do not
provide the operation, stop and report `MISSING_ASSIST` with the missing
operation and exact compiler diagnostic. Source fallback requires explicit
user or orchestrator authorization for a named file and bounded line range.

Create each application-owned source file once. After its first compile
attempt, use the smallest localized patch for each exact diagnostic and
preserve unrelated compiling code. Do not rewrite a complete file as a repair
strategy. Before replacing more than 25% of an existing application file, stop
and report `LARGE_REWRITE_REQUEST` with the file, diagnostic, reason, and
estimated scope. Initial creation and model-driven regeneration are exempt.

## Additional References

Read these only when the task requires them:

* **`RUNTIME_CUSTOM_GUIDE.md`**
  Runtime setup, framework APIs (UserContext, SmartList, WebResponse, etc.), and debugging.

* **`TOOL_API_GUIDE.md`**
  Built-in tool integrations (HTTP client, etc.).