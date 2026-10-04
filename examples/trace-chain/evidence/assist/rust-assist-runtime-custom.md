<!-- ephemeral -->

# TeaQL Rust Runtime Customization

The generated runtime owns provider creation and schema setup. Customize the returned
`UserContext`; do not add provider, policy, or sink parameters to generated execute/save APIs.

```rust
use trace_chain_service_core::{service_runtime, ServiceRuntime, ServiceRuntimeConfig, ServiceRuntimeError};
use std::sync::{Arc, Mutex};
use teaql_runtime::{RequestPolicy, RuntimeError, SafeAuditEvent, SafeAuditEventSink, UserContext};

#[derive(Clone, Default)]
pub struct AppAuditSink {
    events: Arc<Mutex<Vec<SafeAuditEvent>>>,
}

impl AppAuditSink {
    pub fn events(&self) -> Vec<SafeAuditEvent> {
        self.events.lock().expect("App Audit Sink lock poisoned").clone()
    }
}

impl SafeAuditEventSink for AppAuditSink {
    fn on_safe_event(&self, _context: &UserContext, event: &SafeAuditEvent) -> Result<(), RuntimeError> {
        self.events.lock().expect("App Audit Sink lock poisoned").push(event.clone());
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub struct TrustedRequestPolicy;
impl RequestPolicy for TrustedRequestPolicy {}

pub async fn configured_runtime(
    database_url: String,
    app_audit_sink: AppAuditSink,
) -> Result<ServiceRuntime, ServiceRuntimeError> {
    let mut context = service_runtime(ServiceRuntimeConfig { database_url }).await?;
    context.set_request_policy(TrustedRequestPolicy);
    context.set_custom_event_sink(app_audit_sink);
    context.insert_named_resource("trusted_tenant", "system".to_owned());
    Ok(context)
}

pub async fn readiness(context: &ServiceRuntime) -> Result<(), RuntimeError> {
    context.get_named_resource::<String>("trusted_tenant")
        .ok_or_else(|| RuntimeError::Behavior("missing trusted tenant".to_owned()))?;
    context.ensure_schema().await
}

pub fn reject_governance_override(input: &serde_json::Value) -> Result<(), String> {
    const FORBIDDEN: [&str; 6] = [
        "tenant", "provider", "requestPolicy", "auditSink", "hardLimit", "continuousPage",
    ];
    match input {
        serde_json::Value::Object(values) => {
            for (key, value) in values {
                if FORBIDDEN.iter().any(|forbidden| forbidden.eq_ignore_ascii_case(key)) {
                    return Err(format!("forbidden governance override: {key}"));
                }
                reject_governance_override(value)?;
            }
        }
        serde_json::Value::Array(values) => {
            for value in values { reject_governance_override(value)?; }
        }
        _ => {}
    }
    Ok(())
}
```

## Executable contract

## Wrapping the generated Checker registry

`checker_registry()` returns every generated model Checker. Preserve this
registry when adding trusted instrumentation; replacing it with one custom
Checker silently drops validation for the other entity types.

```rust
use trace_chain_service_core::checker_registry;
use teaql_runtime::{Checker, CheckerRegistry, CheckResults, EntityValues,
    InMemoryCheckerRegistry, ObjectLocation};

struct ObservedCheckers(InMemoryCheckerRegistry);
struct ObservedChecker(std::sync::Arc<dyn Checker>);
impl CheckerRegistry for ObservedCheckers {
    fn checker(&self, entity: &str) -> Option<std::sync::Arc<dyn Checker>> {
        self.0.checker(entity).map(|inner|
            std::sync::Arc::new(ObservedChecker(inner)) as std::sync::Arc<dyn Checker>)
    }
}
impl Checker for ObservedChecker {
    fn entity(&self) -> &str { self.0.entity() }
    fn check_and_fix(&self, context: &UserContext, values: &mut EntityValues,
        location: &ObjectLocation, results: &mut CheckResults) {
        // Observe only; delegate unchanged Context, values, location and results.
        self.0.check_and_fix(context, values, location, results);
    }
}

pub fn install_checker_observation(context: &mut UserContext) {
    context.set_checker_registry(ObservedCheckers(checker_registry()));
}
```

Checkers are synchronous callbacks. A test proving overlapping checks must use
real threads, with a bounded synchronization wait, while passing the same original
Context. Two futures joined on one thread do not prove overlapping Checker calls.
Never insert fabricated violations or trace nodes, clear another invocation's
results, or keep mutation/check state on Context. The generated required-field
rules remain authoritative; blank text is not automatically a required-field error.

The generated `ServiceRuntimeExecutor` also provides a trusted diagnostic SPI
for deterministic in-memory checks of successful query metadata. This is off
by default; it is not a business query argument or a safe operator log sink.
The metadata may contain raw binds. Never log, persist or expose those values
through an untrusted transport. Runtime SQL/audit sinks continue to own masking.

```rust
use trace_chain_service_core::ServiceRuntimeExecutor;

let executor = context.require_resource::<ServiceRuntimeExecutor>()?.clone()
    .with_query_metadata_observer(std::sync::Arc::new(|metadata| {
        // Trusted in-memory assertions on actual provider metadata only.
        // Do not print raw bindings or fabricate trace frames.
        assert!(!metadata.backend.is_empty());
    }));
context.insert_resource(executor);
```

Use `trace_chain_service_core::ServiceRuntimeExecutor` for the exact generated
executor type. This replacement keeps the same generated provider and Context;
domain code still passes only `UserContext` to Q/Mutation APIs.

- Workspace startup creates the provider once through `service_runtime_from_env`,
  `service_runtime`, or `service_runtime_from_pool`; provider failures propagate.
- `UserContext` initialization is the trusted boundary for request policy, tenant resources,
  and the customizable App Audit Sink. The immutable raw row audit path remains separate.
- `/health` may be liveness-only; readiness must call the generated schema/provider path and
  fail when a trusted dependency is absent.
- Web, console, and batch workspaces pass only `&UserContext` to generated query/save methods.
- The public write path is the generated `.audit_as(...)` followed by the context-only `save`
  call. Do not bypass it with raw SQL or low-level mutation commands.
- Dynamic JSON and TFP input must reject governance keys recursively. Add negative governance
  tests plus missing dependency, missing intent/audit, and provider failure tests.
- A complete integration test also runs the generated Query/Create Assist against SQLite;
  do not claim runtime customization from a route-only `/health` smoke test.

## Context-bound aggregate document round trips

Use this advanced boundary only when a client opens an Aggregate by Business ID,
edits a projected child graph, and returns it. The Rust runtime owns the opaque
`tqd1` snapshot and the `UserContext` entry points. The application owns the
domain adapter because visibility, writable fields, lifecycle actions, Checker/Fix,
and the transaction cannot be inferred safely from transport JSON alone.

```rust
use async_trait::async_trait;
use teaql_runtime::{
    AcceptedContextualDocument, ContextBoundDocumentService, ContextualDocument,
    DocumentAcceptRequest, DocumentOpenRequest, DocumentRoundTripError,
    TrustedReferencePrincipal, UserContext,
};

pub struct OrderDocumentService;

#[async_trait]
impl ContextBoundDocumentService for OrderDocumentService {
    async fn open_document(
        &self,
        context: &UserContext,
        principal: &TrustedReferencePrincipal,
        request: DocumentOpenRequest,
    ) -> Result<ContextualDocument, DocumentRoundTripError> {
        // 1. Authorize principal + Domain Root + purpose.
        // 2. Load one bounded, complete editing projection through generated Q APIs.
        // 3. Build DocumentSnapshot with loaded/writable fields, relation completeness,
        //    allowed mutations, Aggregate revision, and child versions.
        // 4. Call context.issue_document_snapshot(snapshot, request.lifetime).
        todo!("application-owned Order projection")
    }

    async fn accept_document(
        &self,
        context: &UserContext,
        principal: &TrustedReferencePrincipal,
        request: DocumentAcceptRequest,
    ) -> Result<AcceptedContextualDocument, DocumentRoundTripError> {
        // 1. context.consume_document_snapshot(...) authenticates actor, Domain Root,
        //    service, environment, purpose, Aggregate, lifetime, and the row map.
        // 2. Recheck current authorization and intersect it with the issued writable surface.
        // 3. Reject unknown/read-only fields and cross-document row keys.
        // 4. Treat omission as no change. Remove only an explicit row reference from a
        //    complete writable relation. New rows use clientKey, never a fabricated ref.
        // 5. Check Aggregate + child versions, run Checker/Fix, build one Mutation Ledger,
        //    save atomically with audit intent, then issue a refreshed document.
        todo!("application-owned mutation reconstruction")
    }
}
```

Install the adapter only during trusted runtime assembly:

```rust
context.set_context_bound_document_service(Arc::new(OrderDocumentService));
```

Do not expose `issue_document_snapshot` or `consume_document_snapshot` as remote
utility endpoints. A token is not authorization, a missing child is not deletion,
and decoded persistence identities never become application input. The retained
Order/Items implementation is currently a Rust single-backend POC; do not claim
seven-runtime parity or a published generated adapter.

## Runtime telemetry

Observability is optional and application-owned. Enable the `opentelemetry`
feature, construct `teaql_runtime::OpenTelemetryRuntimeTelemetry` from the
application tracer and meter, wrap it in `Arc`, and call
`context.set_runtime_telemetry(telemetry)`. Keep `NoopRuntimeTelemetry` when it
is absent. The application owns bounded processors, OTLP exporters,
`force_flush` and shutdown; telemetry failure must never change business results.
Installing telemetry does not call `ensure_schema`.
TeaQL derives `teaql.error.category` from the native error type. Sampling never
controls or replaces App Audit Sink delivery. Do not generate a Collector,
additional exporters, auto-discovery, or a telemetry configuration DSL.

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

Capability: `runtime-custom`.

- Keep trusted dependencies and global runtime policy in UserContext initialization.
  Custom providers, policy hooks, and audit sinks must not add execute/save arguments.
- Preserve immutable row audit events and a separate customizable App Audit Sink.
  Include health, integration, and negative governance tests for every customization.
