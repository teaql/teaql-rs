//! Real object-local wire normalization followed by generated audited saves.
//! This is an application composition, not a generated HTTP/TFP endpoint.
use checker_fix_advanced_core::{
    AuditedSave, E, PurchaseOrder, Q, checker_registry, service_runtime_from_pool,
};
use chrono::{TimeZone, Utc};
use serde_json::{Value as Json, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use teaql_core::{Entity, Value};
use teaql_runtime::{
    CheckResults, Checker, CheckerRegistry, EntityValues, FixedBusinessClock,
    InMemoryCheckerRegistry, JsonFieldNamingProfile, MutationDecision, MutationPlan,
    MutationPolicy, MutationPolicyIdentity, ObjectLocation, RuntimeError, SafeAuditEvent,
    SafeAuditEventSink, UserContext,
};
use teaql_tfp_endpoint::{
    NormalizedWireObject, WireEntityMetadata, normalize_wire_object, retain_submitted_paths,
};

static SQL: Mutex<Vec<String>> = Mutex::new(Vec::new());
fn observe(value: &str) {
    SQL.lock().unwrap().push(value.to_owned());
}
#[derive(Clone, Default)]
struct Effects {
    checkers: Arc<AtomicUsize>,
    policies: Arc<AtomicUsize>,
    audits: Arc<AtomicUsize>,
    mappers: Arc<AtomicUsize>,
    saves: Arc<AtomicUsize>,
}
impl Effects {
    fn snapshot(&self) -> [usize; 6] {
        [
            SQL.lock().unwrap().len(),
            self.checkers.load(Ordering::SeqCst),
            self.policies.load(Ordering::SeqCst),
            self.audits.load(Ordering::SeqCst),
            self.mappers.load(Ordering::SeqCst),
            self.saves.load(Ordering::SeqCst),
        ]
    }
    fn delta(&self, before: [usize; 6]) -> [usize; 6] {
        let after = self.snapshot();
        std::array::from_fn(|i| after[i] - before[i])
    }
}
impl SafeAuditEventSink for Effects {
    fn on_safe_event(&self, _: &UserContext, _: &SafeAuditEvent) -> Result<(), RuntimeError> {
        self.audits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
impl MutationPolicy for Effects {
    fn identity(&self) -> MutationPolicyIdentity {
        MutationPolicyIdentity::new("checker-wire-example", "1", "normal-save")
    }
    fn review(&self, _: &UserContext, _: &MutationPlan) -> MutationDecision {
        self.policies.fetch_add(1, Ordering::SeqCst);
        MutationDecision::allow()
    }
}
struct Registry {
    inner: InMemoryCheckerRegistry,
    effects: Effects,
}
struct Delegate {
    inner: Arc<dyn Checker>,
    effects: Effects,
}
impl CheckerRegistry for Registry {
    fn checker(&self, entity: &str) -> Option<Arc<dyn Checker>> {
        self.inner.checker(entity).map(|inner| {
            Arc::new(Delegate {
                inner,
                effects: self.effects.clone(),
            }) as Arc<dyn Checker>
        })
    }
}
impl Checker for Delegate {
    fn entity(&self) -> &str {
        self.inner.entity()
    }
    fn check_and_fix(
        &self,
        context: &UserContext,
        values: &mut EntityValues,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        self.effects.checkers.fetch_add(1, Ordering::SeqCst);
        self.inner.check_and_fix(context, values, location, results);
    }
}
fn mapped(
    context: &UserContext,
    normalized: &NormalizedWireObject,
    effects: &Effects,
) -> PurchaseOrder {
    effects.mappers.fetch_add(1, Ordering::SeqCst);
    let mut row = Q::purchase_orders()
        .comment("Map one validated canonical input object")
        .purpose("Reject ambiguous wire input before creating mutation intent")
        .new_entity(context).expect("reserve example identity");
    match normalized.values.get("contact_phone").unwrap() {
        Json::Null => row.update_contact_phone(Value::Null),
        Json::String(value) => row.update_contact_phone(value.clone()),
        _ => panic!("the bounded fixture supplies string or null only"),
    };
    row.update_internal_note(Value::Null);
    row
}
fn emit(round: usize, profile: &str, name: &str, mut facts: Json) {
    facts["round"] = json!(round);
    facts["profile"] = json!(profile);
    facts["case"] = json!(name);
    facts["passed"] = json!(true);
    println!("RUST_CHECKER_WIRE_CASE={facts}");
}
pub async fn verify() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::var("TEAQL_CHECKER_WIRE_DB")?;
    let round: usize = std::env::var("TEAQL_CHECKER_WIRE_ROUND")?.parse()?;
    let pool = Arc::new(Mutex::new(rusqlite::Connection::open(path)?));
    let mut context = service_runtime_from_pool(pool.clone())
        .await?
        .with_active_root("Tenant", 1);
    context.set_business_clock(FixedBusinessClock::new(
        Utc.with_ymd_and_hms(2032, 2, 29, 10, 15, 30)
            .single()
            .unwrap(),
    ));
    context.ensure_schema().await?;
    let effects = Effects::default();
    context.set_custom_event_sink(effects.clone());
    let policy: Arc<dyn MutationPolicy> = Arc::new(effects.clone());
    context.set_mutation_policy_registry(move |_: &str| Some(policy.clone()));
    context.set_checker_registry(Registry {
        inner: checker_registry(),
        effects: effects.clone(),
    });
    pool.lock().unwrap().trace(Some(observe));
    let profiles = [
        (
            "camelCase",
            "contactPhone",
            JsonFieldNamingProfile::CamelCase,
        ),
        (
            "snake_case",
            "contact_phone",
            JsonFieldNamingProfile::SnakeCase,
        ),
        (
            "PascalCase",
            "ContactPhone",
            JsonFieldNamingProfile::PascalCase,
        ),
    ];
    let mut ids = Vec::new();
    for (profile_index, (profile, wire, _)) in profiles.iter().enumerate() {
        let metadata = WireEntityMetadata::new(
            BTreeMap::from([("contact_phone".to_owned(), (*wire).to_owned())]),
            BTreeMap::from([("legacy/phone~".to_owned(), "contact_phone".to_owned())]),
        )?;
        // Normalize actual JSON first. The optional source path is attached by
        // the existing adapter, never manufactured by a fake Checker result.
        let submitted: Json = serde_json::from_str(r#"{"legacy/phone~":null}"#)?;
        let normalized = normalize_wire_object(submitted.as_object().unwrap(), &metadata)?;
        let row = mapped(&context, &normalized, &effects);
        let before = effects.snapshot();
        effects.saves.fetch_add(1, Ordering::SeqCst);
        let error = row
            .audit_as("Reject a submitted required-null alias")
            .save(&context)
            .await
            .expect_err("required alias must reject");
        let RuntimeError::Check(mut violations) = error else {
            return Err(error.into());
        };
        retain_submitted_paths(&mut violations, &normalized);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].entity_type.as_deref(), Some("PurchaseOrder"));
        assert_eq!(violations[0].location.model_path(), "contact_phone");
        assert_eq!(violations[0].rule.wire_id(), "required");
        assert_eq!(
            violations[0].source_instance_path.as_deref(),
            Some("/legacy~1phone~0")
        );
        let delta = effects.delta(before);
        assert_eq!(
            delta,
            [0, 1, 0, 0, 0, 1],
            "reject before provider/policy/audit"
        );
        let wires: BTreeMap<_, _> = profiles
            .iter()
            .map(|(name, _, naming)| (*name, violations[0].to_wire(*naming)))
            .collect();
        emit(
            round,
            profile,
            "accepted-input-alias",
            json!({"wires":wires,"effects":delta,"idReservedBeforeSaveBoundary":true}),
        );

        for (name, kind, reverse) in [
            ("input-unknown", 0, false),
            ("input-equal-forward", 1, false),
            ("input-equal-reverse", 1, true),
            ("input-unequal-forward", 2, false),
            ("input-unequal-reverse", 2, true),
        ] {
            let canonical = format!(
                "{}:{}",
                serde_json::to_string(wire)?,
                serde_json::to_string("5550100")?
            );
            let alias = format!(
                "\"legacy/phone~\":{}",
                serde_json::to_string(if kind == 2 { "5550199" } else { "5550100" })?
            );
            let text = if kind == 0 {
                r#"{"unknownObject":{"value":1}}"#.to_owned()
            } else if reverse {
                format!("{{{alias},{canonical}}}")
            } else {
                format!("{{{canonical},{alias}}}")
            };
            let submitted: Json = serde_json::from_str(&text)?;
            let before = effects.snapshot();
            let error = normalize_wire_object(submitted.as_object().unwrap(), &metadata)
                .expect_err("ambiguous input must reject before mapping");
            let code = if kind == 0 {
                "WIRE_UNKNOWN_FIELD"
            } else {
                "WIRE_FIELD_COLLISION"
            };
            assert_eq!(error.code(), code);
            assert_eq!(effects.delta(before), [0; 6]);
            emit(
                round,
                profile,
                name,
                json!({"code":code,"effects":[0,0,0,0,0,0],"mapperCalls":0,"saveCalls":0,"checkerCalls":0,
                "submittedJson":text,"noInputFieldOrderAssumption":true}),
            );
        }
        let phone = format!("55509{round}{profile_index}");
        let submitted: Json = serde_json::from_str(&format!(
            "{{{}:{}}}",
            serde_json::to_string(wire)?,
            serde_json::to_string(&phone)?
        ))?;
        let normalized = normalize_wire_object(submitted.as_object().unwrap(), &metadata)?;
        let row = mapped(&context, &normalized, &effects);
        let before = effects.snapshot();
        effects.saves.fetch_add(1, Ordering::SeqCst);
        let stored = row
            .audit_as("Persist one unambiguous wire control")
            .save(&context)
            .await?;
        let delta = effects.delta(before);
        assert!(delta[0] > 0);
        assert_eq!(&delta[1..], &[1, 1, 1, 0, 1]);
        let inserts = SQL.lock().unwrap()[before[0]..]
            .iter()
            .filter(|s| {
                s.trim_start().to_ascii_lowercase().starts_with("insert ")
                    && s.contains("purchase_order_data")
            })
            .count();
        assert_eq!(inserts, 1);
        assert_eq!(
            E::purchase_order(&stored)
                .get_contact_phone()
                .eval()
                .as_deref(),
            Some(phone.as_str())
        );
        assert_eq!(E::purchase_order(&stored).get_version().eval(), Some(1));
        ids.push(stored.id());
        let rows = Q::purchase_orders()
            .with_id_is(stored.id())
            .limit(1)
            .comment("Read back the canonical wire control")
            .purpose("Verify generated E and persisted input agree")
            .execute_for_list(&context)
            .await?;
        assert_eq!(
            E::purchase_order(rows.first().unwrap())
                .get_contact_phone()
                .eval()
                .as_deref(),
            Some(phone.as_str())
        );
        emit(
            round,
            profile,
            "wire-positive-control",
            json!({"id":stored.id(),"phone":phone,"version":1,"providerInserts":inserts,"effects":delta}),
        );
    }
    pool.lock().unwrap().trace(None);
    println!(
        "RUST_CHECKER_WIRE_RESULT={}",
        json!({"language":"rust","round":round,"passed":true,"cases":21,"sameDatabase":true,
        "ids":ids,"realJsonInputAdapter":"existing-public-object-local-composition","recursiveGeneratedHttpOrTfpAccepted":false,"allP0P1Complete":false})
    );
    Ok(())
}
