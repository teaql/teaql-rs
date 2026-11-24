use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use order_management_service_core::teaql_core::{Entity as _, Value};
use order_management_service_core::{
    Q, ServiceRuntimeConfig, request_support::AuditedSave as _, service_runtime,
};
use teaql_runtime::{
    MutationDecision, MutationPlan, MutationPolicy, MutationPolicyApproval,
    MutationPolicyIdentity,
};

const PURPOSE: &str = "Verify customer-owned mutation policy governance";
const ALLOWED_EMAIL: &str = "approved-policy@example.com";
const BLOCKED_EMAIL: &str = "blocked-policy@example.com";

#[derive(Clone)]
struct CustomerMutationPolicy {
    identity: MutationPolicyIdentity,
}

impl MutationPolicy for CustomerMutationPolicy {
    fn identity(&self) -> MutationPolicyIdentity {
        self.identity.clone()
    }

    fn review(
        &self,
        _context: &teaql_runtime::UserContext,
        plan: &MutationPlan,
    ) -> MutationDecision {
        let blocked = plan.operations.iter().any(|operation| {
            matches!(
                operation.changed_values.get("email"),
                Some(Value::Text(email)) if email == BLOCKED_EMAIL
            )
        });
        if blocked {
            MutationDecision::deny(
                "CUSTOMER_EMAIL_BLOCKED",
                "the example policy rejects the reserved email",
                ["Customer.email"],
            )
        } else {
            MutationDecision::allow()
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
    let database = std::env::var_os("TEAQL_EXAMPLE_DATABASE")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join(".local/mutation-policy.db"));
    let mut context = service_runtime(ServiceRuntimeConfig {
        database_url: database.to_string_lossy().into_owned(),
    })
    .await?;
    context.ensure_schema().await?;

    let identity = MutationPolicyIdentity::new(
        "example.customer-write-policy",
        "1",
        "sha256:example-customer-write-policy-v1",
    );
    let policy: Arc<dyn MutationPolicy> = Arc::new(CustomerMutationPolicy {
        identity: identity.clone(),
    });
    context.set_mutation_policy_registry(move |request_key: &str| {
        (request_key == "Customer.saveGraph").then(|| policy.clone())
    });
    context.set_mutation_policy_approval_provider(move |candidate: &MutationPolicyIdentity| {
        (candidate == &identity).then(|| {
            MutationPolicyApproval::new(
                identity.clone(),
                "example-owner",
                SystemTime::UNIX_EPOCH,
            )
        })
    });

    let platform_id = Q::commerce_platforms()
        .order_by_id_asc()
        .limit(1)
        .comment("Load the generated domain root for the mutation-policy probe")
        .purpose(PURPOSE)
        .execute_for_one(&context)
        .await?
        .expect("ensure_schema must seed the generated domain root")
        .id();

    let approved = Q::customers()
        .with_email_is(ALLOWED_EMAIL)
        .comment("Check whether the approved mutation already ran")
        .purpose(PURPOSE)
        .execute_for_one(&context)
        .await?;
    if approved.is_none() {
        let request = Q::customers()
            .comment("Construct the policy-approved Customer")
            .purpose(PURPOSE);
        let mut customer = request.new_entity(&context);
        customer
            .update_name("Approved policy example")
            .update_email(ALLOWED_EMAIL)
            .update_commerce_platform_id(platform_id);
        customer
            .audit_as("Persist the mutation accepted by the approved customer policy")
            .save(&context)
            .await?;
    }

    let request = Q::customers()
        .comment("Construct the policy-denied Customer")
        .purpose(PURPOSE);
    let mut blocked = request.new_entity(&context);
    blocked
        .update_name("Blocked policy example")
        .update_email(BLOCKED_EMAIL)
        .update_commerce_platform_id(platform_id);
    let error = blocked
        .audit_as("Prove that policy denial occurs before persistence")
        .save(&context)
        .await
        .expect_err("the customer-owned policy must reject this mutation");
    assert!(error.to_string().contains("CUSTOMER_EMAIL_BLOCKED"));

    let blocked_rows = Q::customers()
        .with_email_is(BLOCKED_EMAIL)
        .comment("Prove that the denied mutation did not reach the database")
        .purpose(PURPOSE)
        .execute_for_list(&context)
        .await?;
    assert!(blocked_rows.is_empty());

    println!(
        "MUTATION_POLICY_PASS policy=example.customer-write-policy version=1 \
         allowed={ALLOWED_EMAIL} denied={BLOCKED_EMAIL} persisted_denied=0"
    );
    Ok(())
}
