use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use teaql_core::Value;

use crate::{
    EntityKey, EntityRuntimeState, GraphMutationKind, GraphMutationPlan, GraphNode, RuntimeError,
    UserContext,
};

pub const MISSING_MUTATION_POLICY_WARNING: &str = "MUTATION-POLICY-001";
pub const MISSING_MUTATION_POLICY_APPROVAL_WARNING: &str = "MUTATION-POLICY-002";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MutationPolicyIdentity {
    pub id: String,
    pub version: String,
    pub fingerprint: String,
}

impl MutationPolicyIdentity {
    pub fn new(
        id: impl Into<String>,
        version: impl Into<String>,
        fingerprint: impl Into<String>,
    ) -> Self {
        let identity = Self {
            id: id.into(),
            version: version.into(),
            fingerprint: fingerprint.into(),
        };
        assert!(
            !identity.id.trim().is_empty(),
            "policy id must not be blank"
        );
        assert!(
            !identity.version.trim().is_empty(),
            "policy version must not be blank"
        );
        assert!(
            !identity.fingerprint.trim().is_empty(),
            "policy fingerprint must not be blank"
        );
        identity
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationOperationKind {
    Create,
    Update,
    Delete,
    Recover,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MutationOperation {
    pub kind: MutationOperationKind,
    pub entity: EntityKey,
    pub original_version: Option<i64>,
    pub changed_values: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MutationPlan {
    pub execution_id: String,
    pub request_key: String,
    pub root_entity_type: String,
    pub audit_reason: Option<String>,
    pub operations: Vec<MutationOperation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationVerdict {
    Allow,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationDecision {
    pub verdict: MutationVerdict,
    pub code: Option<String>,
    pub message: Option<String>,
    pub field_paths: Vec<String>,
}

impl MutationDecision {
    pub fn allow() -> Self {
        Self {
            verdict: MutationVerdict::Allow,
            code: None,
            message: None,
            field_paths: Vec::new(),
        }
    }

    pub fn deny(
        code: impl Into<String>,
        message: impl Into<String>,
        field_paths: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let code = code.into();
        assert!(
            !code.trim().is_empty(),
            "denied policy code must not be blank"
        );
        Self {
            verdict: MutationVerdict::Deny,
            code: Some(code),
            message: Some(message.into()),
            field_paths: field_paths.into_iter().map(Into::into).collect(),
        }
    }

    pub fn is_allowed(&self) -> bool {
        self.verdict == MutationVerdict::Allow
    }
}

pub trait MutationPolicy: Send + Sync {
    fn identity(&self) -> MutationPolicyIdentity;
    fn review(&self, context: &UserContext, plan: &MutationPlan) -> MutationDecision;
}

pub trait MutationPolicyRegistry: Send + Sync {
    fn resolve(&self, request_key: &str) -> Option<Arc<dyn MutationPolicy>>;
}

impl<F> MutationPolicyRegistry for F
where
    F: Fn(&str) -> Option<Arc<dyn MutationPolicy>> + Send + Sync,
{
    fn resolve(&self, request_key: &str) -> Option<Arc<dyn MutationPolicy>> {
        self(request_key)
    }
}

#[derive(Default)]
pub struct EmptyMutationPolicyRegistry;

impl MutationPolicyRegistry for EmptyMutationPolicyRegistry {
    fn resolve(&self, _request_key: &str) -> Option<Arc<dyn MutationPolicy>> {
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationPolicyApproval {
    pub policy: MutationPolicyIdentity,
    pub approved_by: String,
    pub approved_at: SystemTime,
}

impl MutationPolicyApproval {
    pub fn new(
        policy: MutationPolicyIdentity,
        approved_by: impl Into<String>,
        approved_at: SystemTime,
    ) -> Self {
        let approved_by = approved_by.into();
        assert!(
            !approved_by.trim().is_empty(),
            "mutation policy approver must not be blank"
        );
        Self {
            policy,
            approved_by,
            approved_at,
        }
    }
}

pub trait MutationPolicyApprovalProvider: Send + Sync {
    fn find_approval(&self, policy: &MutationPolicyIdentity) -> Option<MutationPolicyApproval>;
}

impl<F> MutationPolicyApprovalProvider for F
where
    F: Fn(&MutationPolicyIdentity) -> Option<MutationPolicyApproval> + Send + Sync,
{
    fn find_approval(&self, policy: &MutationPolicyIdentity) -> Option<MutationPolicyApproval> {
        self(policy)
    }
}

#[derive(Default)]
pub struct NoMutationPolicyApprovalProvider;

impl MutationPolicyApprovalProvider for NoMutationPolicyApprovalProvider {
    fn find_approval(&self, _policy: &MutationPolicyIdentity) -> Option<MutationPolicyApproval> {
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationPolicySource {
    GeneratedDefault,
    Customer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationPolicyApprovalStatus {
    NotApplicable,
    Missing,
    Approved,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationOperationSummary {
    pub kind: MutationOperationKind,
    pub entity: EntityKey,
    pub changed_fields: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationGovernanceSnapshot {
    pub execution_id: String,
    pub request_key: String,
    pub source: MutationPolicySource,
    pub policy: Option<MutationPolicyIdentity>,
    pub approval_status: MutationPolicyApprovalStatus,
    pub warning_codes: Vec<String>,
    pub operations: Vec<MutationOperationSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationGovernanceEvent {
    pub snapshot: MutationGovernanceSnapshot,
    pub warning_code: String,
    pub first_occurrence: bool,
}

pub trait MutationGovernanceSink: Send + Sync {
    fn on_warning(
        &self,
        context: &UserContext,
        event: &MutationGovernanceEvent,
    ) -> Result<(), RuntimeError>;
}

impl<F> MutationGovernanceSink for F
where
    F: Fn(&UserContext, &MutationGovernanceEvent) -> Result<(), RuntimeError> + Send + Sync,
{
    fn on_warning(
        &self,
        context: &UserContext,
        event: &MutationGovernanceEvent,
    ) -> Result<(), RuntimeError> {
        self(context, event)
    }
}

#[derive(Default)]
pub struct DefaultMutationGovernanceSink;

impl MutationGovernanceSink for DefaultMutationGovernanceSink {
    fn on_warning(
        &self,
        _context: &UserContext,
        event: &MutationGovernanceEvent,
    ) -> Result<(), RuntimeError> {
        if event.first_occurrence {
            eprintln!(
                "[TeaQL][{}] request={} source={:?} approval={:?}",
                event.warning_code,
                event.snapshot.request_key,
                event.snapshot.source,
                event.snapshot.approval_status
            );
        }
        Ok(())
    }
}

tokio::task_local! {
    static ACTIVE_MUTATION_GOVERNANCE: MutationGovernanceSnapshot;
}

pub(crate) async fn with_mutation_governance<F, T>(
    snapshot: MutationGovernanceSnapshot,
    work: F,
) -> T
where
    F: Future<Output = T>,
{
    ACTIVE_MUTATION_GOVERNANCE.scope(snapshot, work).await
}

pub(crate) fn current_mutation_governance() -> Option<MutationGovernanceSnapshot> {
    ACTIVE_MUTATION_GOVERNANCE.try_with(Clone::clone).ok()
}

impl UserContext {
    pub fn with_mutation_policy_registry(
        mut self,
        registry: impl MutationPolicyRegistry + 'static,
    ) -> Self {
        self.mutation_policy_registry = Arc::new(registry);
        self
    }

    pub fn set_mutation_policy_registry(
        &mut self,
        registry: impl MutationPolicyRegistry + 'static,
    ) {
        self.mutation_policy_registry = Arc::new(registry);
    }

    pub fn with_mutation_policy_approval_provider(
        mut self,
        provider: impl MutationPolicyApprovalProvider + 'static,
    ) -> Self {
        self.mutation_policy_approval_provider = Arc::new(provider);
        self
    }

    pub fn set_mutation_policy_approval_provider(
        &mut self,
        provider: impl MutationPolicyApprovalProvider + 'static,
    ) {
        self.mutation_policy_approval_provider = Arc::new(provider);
    }

    pub fn with_mutation_governance_sink(
        mut self,
        sink: impl MutationGovernanceSink + 'static,
    ) -> Self {
        self.mutation_governance_sink = Arc::new(sink);
        self
    }

    pub fn set_mutation_governance_sink(&mut self, sink: impl MutationGovernanceSink + 'static) {
        self.mutation_governance_sink = Arc::new(sink);
    }

    pub fn review_mutation_plan(
        &self,
        plan: &MutationPlan,
    ) -> Result<MutationGovernanceSnapshot, RuntimeError> {
        let resolved = self.mutation_policy_registry.resolve(&plan.request_key);
        let (source, policy, approval_status, warning_codes) = match resolved {
            None => (
                MutationPolicySource::GeneratedDefault,
                None,
                MutationPolicyApprovalStatus::NotApplicable,
                vec![MISSING_MUTATION_POLICY_WARNING.to_owned()],
            ),
            Some(policy) => {
                let identity = policy.identity();
                let decision = policy.review(self, plan);
                if !decision.is_allowed() {
                    return Err(RuntimeError::Policy(format!(
                        "[MUTATION POLICY DENIED] {}: {}",
                        decision.code.as_deref().unwrap_or("MUTATION-POLICY-DENIED"),
                        decision.message.as_deref().unwrap_or("mutation rejected")
                    )));
                }
                let approved = self
                    .mutation_policy_approval_provider
                    .find_approval(&identity)
                    .map(|approval| {
                        approval.policy == identity && !approval.approved_by.trim().is_empty()
                    })
                    .unwrap_or(false);
                (
                    MutationPolicySource::Customer,
                    Some(identity),
                    if approved {
                        MutationPolicyApprovalStatus::Approved
                    } else {
                        MutationPolicyApprovalStatus::Missing
                    },
                    if approved {
                        Vec::new()
                    } else {
                        vec![MISSING_MUTATION_POLICY_APPROVAL_WARNING.to_owned()]
                    },
                )
            }
        };

        let snapshot = MutationGovernanceSnapshot {
            execution_id: plan.execution_id.clone(),
            request_key: plan.request_key.clone(),
            source,
            policy,
            approval_status,
            warning_codes,
            operations: plan
                .operations
                .iter()
                .map(|operation| MutationOperationSummary {
                    kind: operation.kind,
                    entity: operation.entity.clone(),
                    changed_fields: operation.changed_values.keys().cloned().collect(),
                })
                .collect(),
        };

        for warning_code in &snapshot.warning_codes {
            self.emit_mutation_governance_warning(&snapshot, warning_code);
        }
        Ok(snapshot)
    }

    fn emit_mutation_governance_warning(
        &self,
        snapshot: &MutationGovernanceSnapshot,
        warning_code: &str,
    ) {
        let identity = snapshot
            .policy
            .as_ref()
            .map(|identity| {
                format!(
                    "{}:{}:{}",
                    identity.id, identity.version, identity.fingerprint
                )
            })
            .unwrap_or_else(|| "none".to_owned());
        let key = format!("{}|{}|{}", snapshot.request_key, identity, warning_code);
        let first_occurrence = self
            .emitted_mutation_governance_warnings
            .lock()
            .map(|mut emitted| emitted.insert(key))
            .unwrap_or(false);
        let event = MutationGovernanceEvent {
            snapshot: snapshot.clone(),
            warning_code: warning_code.to_owned(),
            first_occurrence,
        };
        if let Err(error) = self.mutation_governance_sink.on_warning(self, &event) {
            eprintln!(
                "[TeaQL][MUTATION-POLICY-SINK-FAILED] request={} error={}",
                snapshot.request_key, error
            );
        }
    }
}

static MUTATION_EXECUTION_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn next_execution_id(context: &UserContext) -> String {
    format!(
        "{}-mutation-{}",
        context.trace_id(),
        MUTATION_EXECUTION_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    )
}

pub(crate) fn graph_policy_plan(
    context: &UserContext,
    plan: &GraphMutationPlan,
) -> Result<MutationPlan, RuntimeError> {
    let root = plan
        .planned_root
        .as_ref()
        .ok_or_else(|| RuntimeError::Graph("graph mutation plan has no planned root".to_owned()))?;
    let operations = plan
        .items
        .iter()
        .filter_map(|item| {
            let kind = match item.kind {
                GraphMutationKind::Create => MutationOperationKind::Create,
                GraphMutationKind::Update => MutationOperationKind::Update,
                GraphMutationKind::Delete => MutationOperationKind::Delete,
                GraphMutationKind::Reference => return None,
            };
            let values: BTreeMap<String, Value> = item.values.clone().into();
            let id = values.get("id").cloned().unwrap_or(Value::I64(0));
            let original_version = values.get("version").and_then(Value::try_i64);
            let changed_values = match kind {
                MutationOperationKind::Update => item
                    .update_fields
                    .iter()
                    .filter_map(|field| {
                        values
                            .get(field)
                            .cloned()
                            .map(|value| (field.clone(), value))
                    })
                    .collect(),
                MutationOperationKind::Delete => BTreeMap::new(),
                _ => values,
            };
            Some(MutationOperation {
                kind,
                entity: EntityKey::new(item.entity.clone(), id),
                original_version,
                changed_values,
            })
        })
        .collect();
    Ok(MutationPlan {
        execution_id: next_execution_id(context),
        request_key: format!("{}.saveGraph", root.entity),
        root_entity_type: root.entity.clone(),
        audit_reason: root.comment.clone(),
        operations,
    })
}

pub(crate) fn ledger_policy_plan(
    context: &UserContext,
    node: &GraphNode,
    root: &EntityRuntimeState,
) -> MutationPlan {
    let changes = root.current_change_set();
    let deleted = root.deleted_keys();
    let new = root.new_keys();
    let mut keys = changes.changes().keys().cloned().collect::<BTreeSet<_>>();
    keys.extend(deleted.iter().cloned());
    let operations = keys
        .into_iter()
        .filter(|key| !(new.contains(key) && deleted.contains(key)))
        .map(|key| {
            let kind = if deleted.contains(&key) {
                MutationOperationKind::Delete
            } else if new.contains(&key) {
                MutationOperationKind::Create
            } else {
                MutationOperationKind::Update
            };
            let changed_values = changes.changes().get(&key).cloned().unwrap_or_default();
            MutationOperation {
                kind,
                original_version: root.get_original_version(&key),
                entity: key,
                changed_values: changed_values.into(),
            }
        })
        .collect();
    MutationPlan {
        execution_id: next_execution_id(context),
        request_key: format!("{}.saveGraph", node.entity),
        root_entity_type: node.entity.clone(),
        audit_reason: root.get_comment().or_else(|| node.comment.clone()),
        operations,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct AllowPolicy(MutationPolicyIdentity);

    impl MutationPolicy for AllowPolicy {
        fn identity(&self) -> MutationPolicyIdentity {
            self.0.clone()
        }

        fn review(&self, _context: &UserContext, _plan: &MutationPlan) -> MutationDecision {
            MutationDecision::allow()
        }
    }

    struct DenyPolicy(MutationPolicyIdentity);

    impl MutationPolicy for DenyPolicy {
        fn identity(&self) -> MutationPolicyIdentity {
            self.0.clone()
        }

        fn review(&self, _context: &UserContext, _plan: &MutationPlan) -> MutationDecision {
            MutationDecision::deny("ORDER-DENIED", "order mutation denied", ["total_amount"])
        }
    }

    fn identity() -> MutationPolicyIdentity {
        MutationPolicyIdentity::new("order-policy", "1.0.0", "sha256:test")
    }

    fn plan() -> MutationPlan {
        MutationPlan {
            execution_id: "exec-1".to_owned(),
            request_key: "Order.saveGraph".to_owned(),
            root_entity_type: "Order".to_owned(),
            audit_reason: Some("submit order".to_owned()),
            operations: vec![MutationOperation {
                kind: MutationOperationKind::Create,
                entity: EntityKey::new("Order", 7_u64),
                original_version: None,
                changed_values: BTreeMap::from([("total_amount".to_owned(), Value::I64(25_000))]),
            }],
        }
    }

    #[test]
    fn generated_default_allows_with_warning_and_deduplicates_log_occurrence() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let capture = events.clone();
        let context = UserContext::default().with_mutation_governance_sink(
            move |_context: &UserContext, event: &MutationGovernanceEvent| {
                capture.lock().unwrap().push(event.clone());
                Ok(())
            },
        );

        let first = context.review_mutation_plan(&plan()).unwrap();
        let second = context.review_mutation_plan(&plan()).unwrap();

        assert_eq!(first.source, MutationPolicySource::GeneratedDefault);
        assert_eq!(first.warning_codes, [MISSING_MUTATION_POLICY_WARNING]);
        assert_eq!(second.warning_codes, [MISSING_MUTATION_POLICY_WARNING]);
        let events = events.lock().unwrap();
        assert!(events[0].first_occurrence);
        assert!(!events[1].first_occurrence);
    }

    #[test]
    fn warning_sink_failure_is_fail_open_for_the_business_save() {
        let context = UserContext::default().with_mutation_governance_sink(
            |_context: &UserContext, _event: &MutationGovernanceEvent| {
                Err(RuntimeError::Event("warning sink unavailable".to_owned()))
            },
        );

        let snapshot = context
            .review_mutation_plan(&plan())
            .expect("warning delivery must not reject an otherwise allowed mutation");

        assert_eq!(snapshot.source, MutationPolicySource::GeneratedDefault);
        assert_eq!(snapshot.warning_codes, [MISSING_MUTATION_POLICY_WARNING]);
    }

    #[test]
    fn customer_policy_requires_an_exact_identity_approval() {
        let policy_identity = identity();
        let policy: Arc<dyn MutationPolicy> = Arc::new(AllowPolicy(policy_identity.clone()));
        let context = UserContext::default()
            .with_mutation_policy_registry(move |_request_key: &str| Some(policy.clone()))
            .with_mutation_policy_approval_provider(move |_identity: &MutationPolicyIdentity| {
                Some(MutationPolicyApproval::new(
                    MutationPolicyIdentity::new("order-policy", "1.0.0", "sha256:other"),
                    "security-review",
                    SystemTime::UNIX_EPOCH,
                ))
            });

        let snapshot = context.review_mutation_plan(&plan()).unwrap();

        assert_eq!(snapshot.source, MutationPolicySource::Customer);
        assert_eq!(
            snapshot.approval_status,
            MutationPolicyApprovalStatus::Missing
        );
        assert_eq!(
            snapshot.warning_codes,
            [MISSING_MUTATION_POLICY_APPROVAL_WARNING]
        );
    }

    #[test]
    fn approved_customer_policy_has_no_warning() {
        let policy_identity = identity();
        let policy: Arc<dyn MutationPolicy> = Arc::new(AllowPolicy(policy_identity.clone()));
        let approval_identity = policy_identity.clone();
        let context = UserContext::default()
            .with_mutation_policy_registry(move |_request_key: &str| Some(policy.clone()))
            .with_mutation_policy_approval_provider(move |_identity: &MutationPolicyIdentity| {
                Some(MutationPolicyApproval::new(
                    approval_identity.clone(),
                    "security-review",
                    SystemTime::UNIX_EPOCH,
                ))
            });

        let snapshot = context.review_mutation_plan(&plan()).unwrap();

        assert_eq!(
            snapshot.approval_status,
            MutationPolicyApprovalStatus::Approved
        );
        assert!(snapshot.warning_codes.is_empty());
    }

    #[test]
    fn denied_customer_policy_fails_before_execution() {
        let policy: Arc<dyn MutationPolicy> = Arc::new(DenyPolicy(identity()));
        let context = UserContext::default()
            .with_mutation_policy_registry(move |_request_key: &str| Some(policy.clone()));
        let persisted = std::sync::atomic::AtomicUsize::new(0);

        let result = context.review_mutation_plan(&plan());
        if result.is_ok() {
            persisted.fetch_add(1, Ordering::SeqCst);
        }

        assert!(
            matches!(result, Err(RuntimeError::Policy(message)) if message.contains("ORDER-DENIED"))
        );
        assert_eq!(persisted.load(Ordering::SeqCst), 0);
    }
}
