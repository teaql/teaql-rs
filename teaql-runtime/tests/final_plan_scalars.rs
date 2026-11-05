//! Public policy-review admission only; no provider writes are attempted.
use std::collections::BTreeMap;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor, Value};
use teaql_runtime::{
    CheckRule, EntityKey, InMemoryMetadataStore, MutationDecision, MutationOperation,
    MutationOperationKind, MutationPlan, MutationPolicy, MutationPolicyIdentity, RuntimeError,
    UserContext,
};

struct ObservingPolicy(Arc<AtomicUsize>);
impl MutationPolicy for ObservingPolicy {
    fn identity(&self) -> MutationPolicyIdentity {
        MutationPolicyIdentity::new("scalar-probe", "1", "public-api-fixture")
    }
    fn review(&self, _: &UserContext, _: &MutationPlan) -> MutationDecision {
        self.0.fetch_add(1, Ordering::SeqCst);
        MutationDecision::allow()
    }
}

fn run_case(data_type: DataType, value: Value, invalid: bool) {
    let calls = Arc::new(AtomicUsize::new(0));
    let policy: Arc<dyn MutationPolicy> = Arc::new(ObservingPolicy(calls.clone()));
    let context = UserContext::new()
        .with_metadata(
            InMemoryMetadataStore::new().with_entity(
                EntityDescriptor::new("TypedCarrier")
                    .property(PropertyDescriptor::new("scalar_value", data_type)),
            ),
        )
        .with_mutation_policy_registry(move |_: &str| Some(policy.clone()));
    let plan = MutationPlan {
        execution_id: "final-plan-probe".into(),
        request_key: "TypedCarrier.saveGraph".into(),
        root_entity_type: "TypedCarrier".into(),
        audit_reason: Some("verify final scalar admission".into()),
        operations: vec![MutationOperation {
            kind: MutationOperationKind::Create,
            entity: EntityKey::new("TypedCarrier", Value::I64(1)),
            original_version: None,
            changed_values: BTreeMap::from([("scalar_value".into(), value)]),
        }],
    };
    let before = format!("{plan:?}"); // NaN is not PartialEq to itself.
    let result = context.review_mutation_plan(&plan);
    if invalid {
        let RuntimeError::Check(results) = result.expect_err("invalid final scalar reached policy")
        else {
            panic!("expected structured checker diagnostics")
        };
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "customer policy saw invalid input"
        );
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].rule, CheckRule::InvalidType);
        assert_eq!(results[0].entity_type.as_deref(), Some("TypedCarrier"));
        assert_eq!(results[0].location.model_path(), "scalar_value");
        assert!(
            results[0]
                .to_wire(teaql_runtime::JsonFieldNamingProfile::CamelCase)
                .input_value
                .is_none()
        );
    } else {
        result.expect("valid final scalar must remain admissible");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    assert_eq!(
        format!("{plan:?}"),
        before,
        "review mutated caller-owned input"
    );
}

macro_rules! scalar_case {
    ($name:ident, $kind:ident, $value:expr, $invalid:expr) => {
        #[test]
        fn $name() {
            run_case(DataType::$kind, $value, $invalid);
        }
    };
}
scalar_case!(final_plan_invalid_bool_integer, Bool, Value::I64(2), true);
scalar_case!(
    final_plan_invalid_bool_text,
    Bool,
    Value::Text("private-invalid-carrier".into()),
    true
);
scalar_case!(
    final_plan_invalid_integral_fraction,
    I64,
    Value::F64(1.5),
    true
);
scalar_case!(
    final_plan_invalid_float_nan,
    F64,
    Value::F64(f64::NAN),
    true
);
scalar_case!(
    final_plan_invalid_float_positive_infinity,
    F64,
    Value::F64(f64::INFINITY),
    true
);
scalar_case!(
    final_plan_invalid_float_negative_infinity,
    F64,
    Value::F64(f64::NEG_INFINITY),
    true
);
scalar_case!(final_plan_valid_bool_true, Bool, Value::Bool(true), false);
scalar_case!(final_plan_valid_bool_false, Bool, Value::Bool(false), false);
scalar_case!(final_plan_valid_integral, I64, Value::I64(7), false);
scalar_case!(final_plan_valid_float, F64, Value::F64(1.25), false);
scalar_case!(final_plan_valid_bool_null, Bool, Value::Null, false);
scalar_case!(final_plan_valid_integral_null, I64, Value::Null, false);
scalar_case!(final_plan_valid_float_null, F64, Value::Null, false);

scalar_case!(
    final_plan_valid_bool_typed_null,
    Bool,
    Value::TypedNull(DataType::Bool),
    false
);
scalar_case!(
    final_plan_valid_integral_typed_null,
    I64,
    Value::TypedNull(DataType::I64),
    false
);
scalar_case!(
    final_plan_valid_float_typed_null,
    F64,
    Value::TypedNull(DataType::F64),
    false
);

#[test]
fn final_plan_fix_runs_once() {
    struct Repair(Arc<AtomicUsize>);
    impl teaql_runtime::Checker for Repair {
        fn entity(&self) -> &str {
            "TypedCarrier"
        }
        fn check_and_fix(
            &self,
            _: &UserContext,
            values: &mut teaql_runtime::EntityValues,
            _: &teaql_runtime::ObjectLocation,
            _: &mut teaql_runtime::CheckResults,
        ) {
            self.0.fetch_add(1, Ordering::SeqCst);
            values.insert("scalar_value".into(), Value::Bool(false));
        }
    }
    let fixes = Arc::new(AtomicUsize::new(0));
    let policies = Arc::new(AtomicUsize::new(0));
    let policy: Arc<dyn MutationPolicy> = Arc::new(ObservingPolicy(policies.clone()));
    let context = UserContext::new()
        .with_metadata(
            InMemoryMetadataStore::new().with_entity(
                EntityDescriptor::new("TypedCarrier")
                    .property(PropertyDescriptor::new("scalar_value", DataType::Bool)),
            ),
        )
        .with_checker_registry(
            teaql_runtime::InMemoryCheckerRegistry::new().with_checker(Repair(fixes.clone())),
        )
        .with_mutation_policy_registry(move |_: &str| Some(policy.clone()));
    let mut values = teaql_runtime::EntityValues::default();
    values.insert("scalar_value".into(), Value::Text("repair me".into()));
    context
        .check_and_fix_values("TypedCarrier", &mut values)
        .unwrap();
    let plan = MutationPlan {
        execution_id: "fix-once".into(),
        request_key: "TypedCarrier.saveGraph".into(),
        root_entity_type: "TypedCarrier".into(),
        audit_reason: Some("retain repaired values".into()),
        operations: vec![MutationOperation {
            kind: MutationOperationKind::Create,
            entity: EntityKey::new("TypedCarrier", Value::I64(1)),
            original_version: None,
            changed_values: values.into(),
        }],
    };
    context.review_mutation_plan(&plan).unwrap();
    assert_eq!(fixes.load(Ordering::SeqCst), 1);
    assert_eq!(policies.load(Ordering::SeqCst), 1);
    assert_eq!(
        plan.operations[0].changed_values["scalar_value"],
        Value::Bool(false)
    );
}

#[test]
fn final_plan_sparse_changes_do_not_repeat_requiredness() {
    let calls = Arc::new(AtomicUsize::new(0));
    let policy: Arc<dyn MutationPolicy> = Arc::new(ObservingPolicy(calls.clone()));
    let context =
        UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(
                EntityDescriptor::new("TypedCarrier").property(
                    PropertyDescriptor::new("unchanged_required", DataType::Text).not_null(),
                ),
            ))
            .with_mutation_policy_registry(move |_: &str| Some(policy.clone()));
    let plan = MutationPlan {
        execution_id: "sparse-plan".into(),
        request_key: "TypedCarrier.saveGraph".into(),
        root_entity_type: "TypedCarrier".into(),
        audit_reason: Some("preserve sparse plan semantics".into()),
        operations: vec![MutationOperation {
            kind: MutationOperationKind::Update,
            entity: EntityKey::new("TypedCarrier", Value::I64(1)),
            original_version: Some(1),
            changed_values: BTreeMap::new(),
        }],
    };
    context.review_mutation_plan(&plan).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(plan.operations[0].changed_values.is_empty());
}
