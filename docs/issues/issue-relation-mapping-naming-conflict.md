# Bug: Relation mapping name conflict with foreign key database column names

## Description
When a relation field has the exact same name as the foreign key database column (for example, relation `channel` mapped to database column `channel` via local key `channel_id`), a deserialization name conflict occurs.

During query executions via `execute_for_list()`, the SQL result returns column `channel` with a numeric value (e.g. `1002`). The deserializer incorrectly maps this flat column to the struct's relation field `channel` (which is an `Option<PaymentChannelType>`). This instantiates a partial, in-memory `PaymentChannelType` entity with default version `0`.

When saving changes on the parent entity, the graph saver recursively inspects the relation `channel`. Because it is `Some(...)` with version `0` in memory, but version `1` exists in the database, it throws:
`optimistic lock conflict on PaymentChannelType(BATCH)`

## Expected Behavior
Flat database columns should only map to foreign key fields (like `channel_id`). Eager relations should only be populated during explicit JOINs, not from flat column mappings.

## Suggested Solution
1. **Naming Conventions**: In code generator modeling templates, enforce renaming relation fields so they do not share the exact name of their database foreign key columns (e.g., rename relation to `payment_channel` or foreign key column to `channel_id`).
2. **Framework Deserialization Safeguard**: Enhance the entity parsing macro code to ensure that fields marked with `#[teaql(relation(...))]` are ignored when matching flat database query column names.
