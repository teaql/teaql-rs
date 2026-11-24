<!-- ephemeral -->

Please help me implement safe generated expression access for `Scalar Type Probe`.

TeaQL expressions preserve three states until evaluation:

- `Value(value)` becomes `Some(value)`.
- `Null` means the field or relation was loaded and is legitimately absent, so
  it becomes `None`.
- `NotLoaded` means the query did not preload required data. This is a coding
  error and `.eval()` intentionally panics with the missing access path.

At a JSON boundary, use `.try_eval()` instead: it returns a Rust `Result` with
a loaded optional or a `teaql_core::eval_location::NotLoadedDiagnostic` error,
without turning NotLoaded into Null. Render the error with `.to_wire()`; `location`
retains canonical KSML segments and `instancePath` is an RFC 6901 JSON Pointer
in the model's naming profile. Do not parse panic text or legacy preload hints.
`attemptedLocation` preserves the complete suffix; `originalExpression` and
`expressionWithBrokenPoint` reconstruct native calls with one `<broken>` at
the first missing node. `suggestedFix.location` identifies what to preload;
discover the matching Q API through Assist instead of guessing method names.

When a default is required only for loaded Null, the underlying generated API
provides `.or_if_null(value)`, `.or_else_if_null(|| value)`, and
`.or_default_if_null()`. These deliberately preserve the same fail-fast behavior for `NotLoaded`.

The following is complete model-derived Rust source, not pseudocode. Scalar,
forward-relation, and generated reverse-list methods are emitted only when they
exist in the generated `E` facade.

```rust
use scalar_type_conformance_service_core::{E, ScalarTypeProbe};
use scalar_type_conformance_service_core::TypePlatform;


pub fn extract_scalar_type_probe_id(entity: &ScalarTypeProbe) -> Option<u64 > {
    E::scalar_type_probe(entity).get_id().eval()
}

pub fn extract_scalar_type_probe_id_or_if_null(entity: &ScalarTypeProbe, fallback: u64) -> u64 {
    E::scalar_type_probe(entity).get_id().or_if_null(fallback)
}

pub fn extract_scalar_type_probe_required_string(entity: &ScalarTypeProbe) -> Option<String > {
    E::scalar_type_probe(entity).get_required_string().eval()
}

pub fn extract_scalar_type_probe_required_string_or_if_null(entity: &ScalarTypeProbe, fallback: String) -> String {
    E::scalar_type_probe(entity).get_required_string().or_if_null(fallback)
}

pub fn extract_scalar_type_probe_optional_string(entity: &ScalarTypeProbe) -> Option<Option<String> > {
    E::scalar_type_probe(entity).get_optional_string().eval()
}

pub fn extract_scalar_type_probe_optional_string_or_if_null(entity: &ScalarTypeProbe, fallback: Option<String>) -> Option<String> {
    E::scalar_type_probe(entity).get_optional_string().or_if_null(fallback)
}

pub fn extract_scalar_type_probe_large_text(entity: &ScalarTypeProbe) -> Option<Option<String> > {
    E::scalar_type_probe(entity).get_large_text().eval()
}

pub fn extract_scalar_type_probe_large_text_or_if_null(entity: &ScalarTypeProbe, fallback: Option<String>) -> Option<String> {
    E::scalar_type_probe(entity).get_large_text().or_if_null(fallback)
}

pub fn extract_scalar_type_probe_required_boolean(entity: &ScalarTypeProbe) -> Option<bool > {
    E::scalar_type_probe(entity).get_required_boolean().eval()
}

pub fn extract_scalar_type_probe_required_boolean_or_if_null(entity: &ScalarTypeProbe, fallback: bool) -> bool {
    E::scalar_type_probe(entity).get_required_boolean().or_if_null(fallback)
}

pub fn extract_scalar_type_probe_optional_boolean(entity: &ScalarTypeProbe) -> Option<Option<bool> > {
    E::scalar_type_probe(entity).get_optional_boolean().eval()
}

pub fn extract_scalar_type_probe_optional_boolean_or_if_null(entity: &ScalarTypeProbe, fallback: Option<bool>) -> Option<bool> {
    E::scalar_type_probe(entity).get_optional_boolean().or_if_null(fallback)
}

pub fn extract_scalar_type_probe_required_integer(entity: &ScalarTypeProbe) -> Option<i64 > {
    E::scalar_type_probe(entity).get_required_integer().eval()
}

pub fn extract_scalar_type_probe_required_integer_or_if_null(entity: &ScalarTypeProbe, fallback: i64) -> i64 {
    E::scalar_type_probe(entity).get_required_integer().or_if_null(fallback)
}

pub fn extract_scalar_type_probe_optional_integer(entity: &ScalarTypeProbe) -> Option<Option<i64> > {
    E::scalar_type_probe(entity).get_optional_integer().eval()
}

pub fn extract_scalar_type_probe_optional_integer_or_if_null(entity: &ScalarTypeProbe, fallback: Option<i64>) -> Option<i64> {
    E::scalar_type_probe(entity).get_optional_integer().or_if_null(fallback)
}

pub fn extract_scalar_type_probe_required_long(entity: &ScalarTypeProbe) -> Option<i64 > {
    E::scalar_type_probe(entity).get_required_long().eval()
}

pub fn extract_scalar_type_probe_required_long_or_if_null(entity: &ScalarTypeProbe, fallback: i64) -> i64 {
    E::scalar_type_probe(entity).get_required_long().or_if_null(fallback)
}

pub fn extract_scalar_type_probe_optional_long(entity: &ScalarTypeProbe) -> Option<Option<i64> > {
    E::scalar_type_probe(entity).get_optional_long().eval()
}

pub fn extract_scalar_type_probe_optional_long_or_if_null(entity: &ScalarTypeProbe, fallback: Option<i64>) -> Option<i64> {
    E::scalar_type_probe(entity).get_optional_long().or_if_null(fallback)
}

pub fn extract_scalar_type_probe_required_decimal(entity: &ScalarTypeProbe) -> Option<rust_decimal::Decimal > {
    E::scalar_type_probe(entity).get_required_decimal().eval()
}

pub fn extract_scalar_type_probe_required_decimal_or_if_null(entity: &ScalarTypeProbe, fallback: rust_decimal::Decimal) -> rust_decimal::Decimal {
    E::scalar_type_probe(entity).get_required_decimal().or_if_null(fallback)
}

pub fn extract_scalar_type_probe_optional_decimal(entity: &ScalarTypeProbe) -> Option<Option<rust_decimal::Decimal> > {
    E::scalar_type_probe(entity).get_optional_decimal().eval()
}

pub fn extract_scalar_type_probe_optional_decimal_or_if_null(entity: &ScalarTypeProbe, fallback: Option<rust_decimal::Decimal>) -> Option<rust_decimal::Decimal> {
    E::scalar_type_probe(entity).get_optional_decimal().or_if_null(fallback)
}

pub fn extract_scalar_type_probe_required_float(entity: &ScalarTypeProbe) -> Option<f32 > {
    E::scalar_type_probe(entity).get_required_float().eval()
}

pub fn extract_scalar_type_probe_required_float_or_if_null(entity: &ScalarTypeProbe, fallback: f32) -> f32 {
    E::scalar_type_probe(entity).get_required_float().or_if_null(fallback)
}

pub fn extract_scalar_type_probe_optional_float(entity: &ScalarTypeProbe) -> Option<Option<f32> > {
    E::scalar_type_probe(entity).get_optional_float().eval()
}

pub fn extract_scalar_type_probe_optional_float_or_if_null(entity: &ScalarTypeProbe, fallback: Option<f32>) -> Option<f32> {
    E::scalar_type_probe(entity).get_optional_float().or_if_null(fallback)
}

pub fn extract_scalar_type_probe_required_double(entity: &ScalarTypeProbe) -> Option<f64 > {
    E::scalar_type_probe(entity).get_required_double().eval()
}

pub fn extract_scalar_type_probe_required_double_or_if_null(entity: &ScalarTypeProbe, fallback: f64) -> f64 {
    E::scalar_type_probe(entity).get_required_double().or_if_null(fallback)
}

pub fn extract_scalar_type_probe_optional_double(entity: &ScalarTypeProbe) -> Option<Option<f64> > {
    E::scalar_type_probe(entity).get_optional_double().eval()
}

pub fn extract_scalar_type_probe_optional_double_or_if_null(entity: &ScalarTypeProbe, fallback: Option<f64>) -> Option<f64> {
    E::scalar_type_probe(entity).get_optional_double().or_if_null(fallback)
}

pub fn extract_scalar_type_probe_required_date(entity: &ScalarTypeProbe) -> Option<chrono::NaiveDate > {
    E::scalar_type_probe(entity).get_required_date().eval()
}

pub fn extract_scalar_type_probe_required_date_or_if_null(entity: &ScalarTypeProbe, fallback: chrono::NaiveDate) -> chrono::NaiveDate {
    E::scalar_type_probe(entity).get_required_date().or_if_null(fallback)
}

pub fn extract_scalar_type_probe_optional_date(entity: &ScalarTypeProbe) -> Option<Option<chrono::NaiveDate> > {
    E::scalar_type_probe(entity).get_optional_date().eval()
}

pub fn extract_scalar_type_probe_optional_date_or_if_null(entity: &ScalarTypeProbe, fallback: Option<chrono::NaiveDate>) -> Option<chrono::NaiveDate> {
    E::scalar_type_probe(entity).get_optional_date().or_if_null(fallback)
}

pub fn extract_scalar_type_probe_required_time(entity: &ScalarTypeProbe) -> Option<String > {
    E::scalar_type_probe(entity).get_required_time().eval()
}

pub fn extract_scalar_type_probe_required_time_or_if_null(entity: &ScalarTypeProbe, fallback: String) -> String {
    E::scalar_type_probe(entity).get_required_time().or_if_null(fallback)
}

pub fn extract_scalar_type_probe_optional_time(entity: &ScalarTypeProbe) -> Option<Option<String> > {
    E::scalar_type_probe(entity).get_optional_time().eval()
}

pub fn extract_scalar_type_probe_optional_time_or_if_null(entity: &ScalarTypeProbe, fallback: Option<String>) -> Option<String> {
    E::scalar_type_probe(entity).get_optional_time().or_if_null(fallback)
}

pub fn extract_scalar_type_probe_required_datetime(entity: &ScalarTypeProbe) -> Option<teaql_core::time::Timestamp > {
    E::scalar_type_probe(entity).get_required_datetime().eval()
}

pub fn extract_scalar_type_probe_required_datetime_or_if_null(entity: &ScalarTypeProbe, fallback: teaql_core::time::Timestamp) -> teaql_core::time::Timestamp {
    E::scalar_type_probe(entity).get_required_datetime().or_if_null(fallback)
}

pub fn extract_scalar_type_probe_optional_datetime(entity: &ScalarTypeProbe) -> Option<Option<teaql_core::time::Timestamp> > {
    E::scalar_type_probe(entity).get_optional_datetime().eval()
}

pub fn extract_scalar_type_probe_optional_datetime_or_if_null(entity: &ScalarTypeProbe, fallback: Option<teaql_core::time::Timestamp>) -> Option<teaql_core::time::Timestamp> {
    E::scalar_type_probe(entity).get_optional_datetime().or_if_null(fallback)
}

pub fn extract_scalar_type_probe_create_time(entity: &ScalarTypeProbe) -> Option<teaql_core::time::Timestamp > {
    E::scalar_type_probe(entity).get_create_time().eval()
}

pub fn extract_scalar_type_probe_create_time_or_if_null(entity: &ScalarTypeProbe, fallback: teaql_core::time::Timestamp) -> teaql_core::time::Timestamp {
    E::scalar_type_probe(entity).get_create_time().or_if_null(fallback)
}

pub fn extract_scalar_type_probe_update_time(entity: &ScalarTypeProbe) -> Option<teaql_core::time::Timestamp > {
    E::scalar_type_probe(entity).get_update_time().eval()
}

pub fn extract_scalar_type_probe_update_time_or_if_null(entity: &ScalarTypeProbe, fallback: teaql_core::time::Timestamp) -> teaql_core::time::Timestamp {
    E::scalar_type_probe(entity).get_update_time().or_if_null(fallback)
}

pub fn extract_scalar_type_probe_version(entity: &ScalarTypeProbe) -> Option<i64 > {
    E::scalar_type_probe(entity).get_version().eval()
}

pub fn extract_scalar_type_probe_version_or_if_null(entity: &ScalarTypeProbe, fallback: i64) -> i64 {
    E::scalar_type_probe(entity).get_version().or_if_null(fallback)
}

pub fn extract_scalar_type_probe_type_platform_id(entity: &ScalarTypeProbe) -> Option<u64 > {
    E::scalar_type_probe(entity).get_type_platform_id().eval()
}

pub fn traverse_scalar_type_probe_type_platform<'a>(entity: &'a ScalarTypeProbe) -> Option<&'a TypePlatform> {
    E::scalar_type_probe(entity).get_type_platform().eval()
}


```

Use only the generated functions above. Select every traversed field or
relation in the corresponding query. A loaded null/list miss may be handled as
`None`; do not catch or default a NotLoaded panic.

## Typed entity JSON boundary

Generated `module()` variants and `service_runtime(...)` install immutable,
model-owned JSON metadata. Use the context codec, not the legacy presentation
serializer, when crossing a typed JSON boundary:

```rust
use scalar_type_conformance_service_core::{generated_json_field_naming_profile, ScalarTypeProbe};
use teaql_runtime::{entity_json, JsonFieldNamingProfile, UserContext};

pub fn encode_scalar_type_probe(context: &UserContext, entity: &ScalarTypeProbe) -> Result<String, entity_json::Error> {
    context.encode_entity_json(entity, generated_json_field_naming_profile())
}

pub fn decode_scalar_type_probe(context: &UserContext, payload: &str) -> Result<ScalarTypeProbe, entity_json::Error> {
    context.decode_entity_json::<ScalarTypeProbe>(payload, generated_json_field_naming_profile())
}
```

Select the same naming profile for encode and decode: `CamelCase` (default),
`SnakeCase`, or `PascalCase`. For the model's default, use
`generated_json_field_naming_profile()` on both sides. Long, ID, version and exact
Decimal values use strings; Integer is a bounded i32 JSON number. Float and Double
must be finite. Date, Time and DateTime retain their distinct semantic types.
Omitted fields remain NotLoaded; explicit nullable values remain loaded Null.
Forward FK-only values do not claim the related object was loaded. Inverse
relations retain object/null/omitted and list/empty/omitted distinctions.
Duplicate aliases, unknown fields, invalid scalars and private runtime state are
rejected with value-free structured errors (`to_wire()`). JSON hydration is not
authorization, an authenticated editing snapshot, or permission to save supplied
values; the application must still enforce request policy and audited saves.
>>


---

## TeaQL seven-language assist contract

Apply the verified Rust semantic ceiling while using only the exact RUST generated and
runtime APIs. Discover APIs through the generated application AGENTS.md and progressive
model-aware Assist. Do not inspect generated domain-library source.

- Do not create plurals by appending `s` or `es`; use the centralized generated plural.
- Human and non-human entities use different generated predicate vocabularies. Preserve
  forms such as “who are active” and “whose email is”; never infer them from English.
- Configure filters, projection, paging, and other query options before `purpose(...)`.
  Comment may appear anywhere in the chain. Purpose enters the executable stage; execution
  requires both values, but comment does not have to immediately precede purpose.
- Every execute/list/stream and every save accepts exactly one context argument:
  `UserContext`. Name that argument `context`, never `runtime`; data services and global
  policy are injected when the context is built. Reserve `runtime` for process-level
  runtime ownership, provider/pool setup, and module assembly.
- Tenant, merchant, identity, permissions, request policy, purpose policy, hard limit,
  and continuous-page cursor policy come only from trusted context, never dynamic JSON or TFP.
- If the required operation is absent after current entity/action and required field
  Assist, stop that path and report MISSING_ASSIST. Do not guess an API or search the
  generated library as a fallback.
- Create each application-owned source file once. After its first compile attempt,
  repair only the smallest block identified by the exact compiler or test diagnostic.
  Preserve unrelated code; do not rewrite the complete file as an error-recovery loop.
- Before a repair that would replace more than 25% of an existing application file,
  stop and report LARGE_REWRITE_REQUEST with the file, exact diagnostic, reason, and
  estimated scope. Initial creation and model-driven regeneration are not repairs.

Capability: `expression`.

- Distinguish a loaded null from a field or relation that was not projected. A
  NotLoaded/coding error must remain visible; do not turn it into an ordinary null.
- Select every traversed relation first and use the generated E/expression API for
  scalar, object, and list traversal. Do not translate Java accessor names by guess.
