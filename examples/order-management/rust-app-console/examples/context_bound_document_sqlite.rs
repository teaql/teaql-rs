use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::NaiveDate;
use order_management_service_core::teaql_core::Entity as _;
use order_management_service_core::{
    CustomerOrder, DataServiceExecutor, E, OrderLine, Q, ServiceRuntimeConfig,
    request_support::AuditedSave as _, service_runtime,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use teaql_runtime::{
    AcceptedContextualDocument, ContextBoundDocumentService, ContextBoundReferenceRuntime,
    ContextualDocument, DeploymentProfile, DocumentAcceptRequest, DocumentEntitySnapshot,
    DocumentMutationKind, DocumentOpenRequest, DocumentRelationCompleteness,
    DocumentRelationSnapshot, DocumentRoundTripError, DocumentSnapshot, ReferenceDocumentScope,
    ReferenceIdentity, ReferenceKey, RoundTripReferenceError, StaticReferenceKeyProvider,
    SubmittedContextualDocument, TrustedReferencePrincipal, UserContext,
};

const PURPOSE: &str = "edit-generated-order-items";
const MODEL_FINGERPRINT: &str = "order-management-generated-v1";
const ORDER_NUMBER: &str = "DOC-SQLITE-1001";

#[derive(Clone)]
struct GeneratedOrderDocumentService {
    permissions: Arc<Mutex<BTreeSet<String>>>,
}

fn required<T>(value: Option<T>) -> Result<T, DocumentRoundTripError> {
    value.ok_or_else(DocumentRoundTripError::projection_violation)
}

impl GeneratedOrderDocumentService {
    fn require_access(
        &self,
        principal: &TrustedReferencePrincipal,
    ) -> Result<(), DocumentRoundTripError> {
        if self
            .permissions
            .lock()
            .unwrap()
            .contains(&principal.subject)
        {
            Ok(())
        } else {
            Err(DocumentRoundTripError::authorization_required())
        }
    }

    async fn load_order(
        &self,
        context: &UserContext,
        order_number: &str,
    ) -> Result<CustomerOrder, DocumentRoundTripError> {
        Q::customer_orders()
            .with_order_number_is(order_number)
            .select_order_line_list_with(
                Q::order_lines()
                    .select_self_fields()
                    .order_by_id_asc()
                    .limit(100),
            )
            .limit(2)
            .comment("what: load a generated Order and its complete item projection")
            .purpose("why: open or validate a context-bound editable document")
            .execute_for_one(context)
            .await
            .map_err(|_| DocumentRoundTripError::validation_failed())?
            .ok_or_else(DocumentRoundTripError::not_found)
    }

    fn validate_owner(
        &self,
        principal: &TrustedReferencePrincipal,
        order: &CustomerOrder,
    ) -> Result<(), DocumentRoundTripError> {
        let customer_id = required(E::customer_order(order).get_customer_id().eval())?;
        if principal.domain_root_type == "Customer" && principal.domain_root_id == customer_id {
            Ok(())
        } else {
            Err(DocumentRoundTripError::authorization_required())
        }
    }

    fn render(
        &self,
        context: &UserContext,
        principal: &TrustedReferencePrincipal,
        order: &CustomerOrder,
        lifetime: Duration,
    ) -> Result<ContextualDocument, DocumentRoundTripError> {
        self.require_access(principal)?;
        self.validate_owner(principal, order)?;
        let order_id = required(E::customer_order(order).get_id().eval())?;
        let order_version = required(E::customer_order(order).get_version().eval())?;
        let order_number = required(E::customer_order(order).get_order_number().eval())?;
        let lines = required(E::customer_order(order).get_order_line_list().eval())?;
        let mut entities = Vec::new();
        let mut row_keys = Vec::new();
        let mut body_lines = Vec::new();
        for (index, line) in lines.iter().enumerate() {
            let line_id = required(E::order_line(line).get_id().eval())?;
            let line_version = required(E::order_line(line).get_version().eval())?;
            let product_name = required(E::order_line(line).get_product_name().eval())?;
            let sku = required(E::order_line(line).get_sku().eval())?;
            let quantity = required(E::order_line(line).get_quantity().eval())?;
            let row_key = format!("item:{index}");
            row_keys.push(row_key.clone());
            entities.push(DocumentEntitySnapshot::new(
                row_key.clone(),
                ReferenceIdentity::new("OrderLine", line_id, line_version)?,
                ["product_name", "sku", "quantity"],
                ["quantity"],
                [
                    DocumentMutationKind::Update,
                    DocumentMutationKind::RemoveChild,
                ],
            )?);
            body_lines.push(json!({
                "ref": row_key,
                "productName": product_name,
                "sku": sku,
                "quantity": quantity
            }));
        }
        let scope = ReferenceDocumentScope::new(
            format!("order-{order_id}-version-{order_version}"),
            PURPOSE,
            "CustomerOrder",
            order_id,
            order_version,
        )?;
        let snapshot = DocumentSnapshot::new(
            MODEL_FINGERPRINT,
            order_number.clone(),
            scope,
            ReferenceIdentity::new("CustomerOrder", order_id, order_version)?,
            entities,
            vec![DocumentRelationSnapshot::new(
                "order_line_list",
                row_keys,
                DocumentRelationCompleteness::Complete,
                true,
                [DocumentMutationKind::RemoveChild],
            )?],
        )?;
        Ok(ContextualDocument {
            business_id: order_number.clone(),
            document_token: context.issue_document_snapshot(snapshot, lifetime)?,
            body: json!({"orderNumber": order_number, "items": body_lines}),
        })
    }

    fn submitted_rows(body: &Value) -> Result<&Vec<Value>, DocumentRoundTripError> {
        body.get("items")
            .and_then(Value::as_array)
            .ok_or_else(DocumentRoundTripError::projection_violation)
    }

    fn removed_refs(body: &Value) -> Result<Vec<&str>, DocumentRoundTripError> {
        body.get("removedRefs")
            .map(|value| {
                value
                    .as_array()
                    .ok_or_else(DocumentRoundTripError::projection_violation)?
                    .iter()
                    .map(|entry| {
                        entry
                            .as_str()
                            .ok_or_else(DocumentRoundTripError::projection_violation)
                    })
                    .collect()
            })
            .unwrap_or_else(|| Ok(Vec::new()))
    }
}

#[async_trait]
impl ContextBoundDocumentService for GeneratedOrderDocumentService {
    async fn open_document(
        &self,
        context: &UserContext,
        principal: &TrustedReferencePrincipal,
        request: DocumentOpenRequest,
    ) -> Result<ContextualDocument, DocumentRoundTripError> {
        self.require_access(principal)?;
        if request.aggregate_type != "CustomerOrder" || request.purpose != PURPOSE {
            return Err(DocumentRoundTripError::scope_mismatch());
        }
        let order = self.load_order(context, &request.business_id).await?;
        self.render(context, principal, &order, request.lifetime)
    }

    async fn accept_document(
        &self,
        context: &UserContext,
        principal: &TrustedReferencePrincipal,
        request: DocumentAcceptRequest,
    ) -> Result<AcceptedContextualDocument, DocumentRoundTripError> {
        self.require_access(principal)?;
        if request.expected_aggregate_type != "CustomerOrder" || request.purpose != PURPOSE {
            return Err(DocumentRoundTripError::scope_mismatch());
        }
        let verified = context.consume_document_snapshot(
            &request.submitted.document_token,
            &request.expected_aggregate_type,
            &request.purpose,
        )?;
        if request.submitted.business_id != verified.snapshot.business_id
            || verified.snapshot.model_fingerprint != MODEL_FINGERPRINT
            || request
                .submitted
                .body
                .get("orderNumber")
                .and_then(Value::as_str)
                != Some(verified.snapshot.business_id.as_str())
        {
            return Err(DocumentRoundTripError::projection_violation());
        }

        let mut order = self
            .load_order(context, &verified.snapshot.business_id)
            .await?;
        self.validate_owner(principal, &order)?;
        let order_id = required(E::customer_order(&order).get_id().eval())?;
        let order_version = required(E::customer_order(&order).get_version().eval())?;
        if order_id != verified.snapshot.aggregate.id
            || order_version != verified.snapshot.aggregate.version
        {
            return Err(DocumentRoundTripError::revision_conflict());
        }
        let total_amount = required(E::customer_order(&order).get_total_amount().eval())?;
        let lines = required(E::customer_order(&order).get_order_line_list().eval())?;
        let mut current: BTreeMap<u64, OrderLine> = lines
            .iter()
            .cloned()
            .map(|line| {
                let id = E::order_line(&line)
                    .get_id()
                    .eval()
                    .expect("fully selected generated OrderLine has an ID");
                (id, line)
            })
            .collect();
        let relation = verified
            .snapshot
            .relation("order_line_list")
            .ok_or_else(DocumentRoundTripError::projection_violation)?;
        let relation_rows: BTreeSet<&str> = relation.row_keys.iter().map(String::as_str).collect();
        let mut submitted_refs = BTreeSet::new();
        let mut changed = Vec::new();

        for row in Self::submitted_rows(&request.submitted.body)? {
            let object = row
                .as_object()
                .ok_or_else(DocumentRoundTripError::projection_violation)?;
            if object
                .keys()
                .any(|key| !matches!(key.as_str(), "ref" | "productName" | "sku" | "quantity"))
            {
                return Err(DocumentRoundTripError::projection_violation());
            }
            let row_key = object
                .get("ref")
                .and_then(Value::as_str)
                .ok_or_else(DocumentRoundTripError::projection_violation)?;
            if !submitted_refs.insert(row_key) || !relation_rows.contains(row_key) {
                return Err(DocumentRoundTripError::projection_violation());
            }
            let issued = verified
                .snapshot
                .entity(row_key)
                .ok_or_else(DocumentRoundTripError::projection_violation)?;
            let line = current
                .get_mut(&issued.identity.id)
                .ok_or_else(DocumentRoundTripError::scope_mismatch)?;
            let current_version = required(E::order_line(line).get_version().eval())?;
            if current_version != issued.identity.version {
                return Err(DocumentRoundTripError::revision_conflict());
            }
            if object.get("productName").and_then(Value::as_str)
                != E::order_line(line).get_product_name().eval().as_deref()
                || object.get("sku").and_then(Value::as_str)
                    != E::order_line(line).get_sku().eval().as_deref()
            {
                return Err(DocumentRoundTripError::projection_violation());
            }
            let quantity = object
                .get("quantity")
                .and_then(Value::as_i64)
                .filter(|value| *value > 0)
                .ok_or_else(DocumentRoundTripError::validation_failed)?;
            if E::order_line(line).get_quantity().eval() != Some(quantity) {
                line.update_quantity(quantity);
                changed.push(line.clone());
            }
        }

        let mut removed = Vec::new();
        let mut unique_removed = BTreeSet::new();
        for row_key in Self::removed_refs(&request.submitted.body)? {
            if relation.completeness != DocumentRelationCompleteness::Complete
                || !relation.writable
                || !relation_rows.contains(row_key)
                || submitted_refs.contains(row_key)
                || !unique_removed.insert(row_key)
                || !relation
                    .allowed_mutations
                    .contains(&DocumentMutationKind::RemoveChild)
            {
                return Err(DocumentRoundTripError::projection_violation());
            }
            let issued = verified
                .snapshot
                .entity(row_key)
                .ok_or_else(DocumentRoundTripError::projection_violation)?;
            let mut line = current
                .remove(&issued.identity.id)
                .ok_or_else(DocumentRoundTripError::scope_mismatch)?;
            if required(E::order_line(&line).get_version().eval())? != issued.identity.version {
                return Err(DocumentRoundTripError::revision_conflict());
            }
            line.mark_for_deletion();
            removed.push(line);
        }

        let previous_revision = order_version;
        let (new_revision, changed_entities) = if changed.is_empty() && removed.is_empty() {
            (order_version, Vec::new())
        } else {
            order.update_total_amount(total_amount);
            context
                .execute_in_send_transaction::<DataServiceExecutor, _, _>(|scope| {
                    Box::pin(async move {
                        let saved_order = scope
                            .save_audited(order.audit_as(format!(
                                "accept context-bound command {}",
                                request.submitted.command_id
                            )))
                            .await?;
                        let mut identities = Vec::new();
                        for line in changed {
                            let saved = scope
                                .save_audited(
                                    line.audit_as("update generated projected order item"),
                                )
                                .await?;
                            identities.push(
                                ReferenceIdentity::new("OrderLine", saved.id(), saved.version())
                                    .map_err(|error| {
                                        teaql_runtime::RuntimeError::Graph(error.to_string())
                                    })?,
                            );
                        }
                        for line in removed {
                            let id = line.id();
                            scope
                                .save_audited(
                                    line.audit_as("remove generated projected order item"),
                                )
                                .await?;
                            identities.push(ReferenceIdentity::new("OrderLine", id, 0).map_err(
                                |error| teaql_runtime::RuntimeError::Graph(error.to_string()),
                            )?);
                        }
                        Ok((saved_order.version(), identities))
                    })
                })
                .await
                .map_err(|_| DocumentRoundTripError::validation_failed())?
        };
        let refreshed = self
            .load_order(context, &verified.snapshot.business_id)
            .await?;
        Ok(AcceptedContextualDocument {
            document: self.render(context, principal, &refreshed, Duration::from_secs(900))?,
            previous_revision,
            new_revision,
            changed_entities,
        })
    }
}

fn reference_runtime(
    permissions: Arc<Mutex<BTreeSet<String>>>,
    customer_id: u64,
) -> Arc<ContextBoundReferenceRuntime> {
    let authorize = move |principal: &TrustedReferencePrincipal,
                          scope: &ReferenceDocumentScope,
                          _identity: &ReferenceIdentity|
          -> Result<(), RoundTripReferenceError> {
        if permissions.lock().unwrap().contains(&principal.subject)
            && principal.domain_root_type == "Customer"
            && principal.domain_root_id == customer_id
            && scope.aggregate_type == "CustomerOrder"
        {
            Ok(())
        } else {
            Err(RoundTripReferenceError::authorization_required())
        }
    };
    Arc::new(
        ContextBoundReferenceRuntime::governed(
            DeploymentProfile::Test,
            "generated-order-service",
            "sqlite-e2e",
            Arc::new(
                StaticReferenceKeyProvider::new(
                    "k1",
                    [ReferenceKey::new("k1", [0x47; 32]).unwrap()],
                )
                .unwrap(),
            ),
            Arc::new(authorize),
        )
        .unwrap(),
    )
}

async fn application_context(
    database_url: &str,
    subject: &str,
    customer_id: u64,
    permissions: Arc<Mutex<BTreeSet<String>>>,
    service: Arc<GeneratedOrderDocumentService>,
) -> Result<UserContext, Box<dyn std::error::Error>> {
    let context = service_runtime(ServiceRuntimeConfig {
        database_url: database_url.to_owned(),
    })
    .await?;
    Ok(context
        .with_round_trip_reference_runtime(reference_runtime(permissions, customer_id))
        .with_trusted_reference_principal(
            TrustedReferencePrincipal::new("oidc", subject, "Customer", customer_id).unwrap(),
        )
        .with_context_bound_document_service(service))
}

struct SeededIds {
    customer_id: u64,
    order_id: u64,
    first_line_id: u64,
    second_line_id: u64,
}

async fn seed(context: &UserContext) -> Result<SeededIds, Box<dyn std::error::Error>> {
    context.ensure_schema().await?;
    let platform_id = Q::commerce_platforms()
        .limit(1)
        .comment("what: load the generated root seeded by ensure schema")
        .purpose("why: attach the SQLite document fixture to its domain root")
        .execute_for_one(context)
        .await?
        .expect("ensure schema must seed the commerce platform")
        .id();

    let mut customer = Q::customers()
        .comment("what: construct the document owner")
        .purpose("why: seed the generated SQLite document fixture")
        .new_entity(context);
    customer
        .update_name("Context document customer")
        .update_email("context-document@example.test")
        .update_commerce_platform_id(platform_id);
    let customer_id = customer.id();
    customer
        .audit_as("seed context-bound document owner")
        .save(context)
        .await?;

    let mut product = Q::products()
        .comment("what: construct the document product")
        .purpose("why: seed generated OrderLine relations")
        .new_entity(context);
    product
        .update_name("Context document product")
        .update_sku("DOC-SQLITE-SKU")
        .update_commerce_platform_id(platform_id);
    let product_id = product.id();
    product
        .audit_as("seed context-bound document product")
        .save(context)
        .await?;

    let mut order = Q::customer_orders()
        .comment("what: construct the editable generated Order")
        .purpose("why: seed the SQLite context-bound aggregate")
        .new_entity(context);
    order
        .update_order_number(ORDER_NUMBER)
        .update_order_date(NaiveDate::from_ymd_opt(2026, 10, 1).unwrap())
        .update_total_amount(Decimal::new(2500, 2))
        .update_status_to_pending()
        .update_customer_id(customer_id)
        .update_commerce_platform_id(platform_id);
    let order_id = order.id();
    order
        .audit_as("seed context-bound generated Order")
        .save(context)
        .await?;

    let mut first = Q::order_lines()
        .comment("what: construct the first editable generated OrderLine")
        .purpose("why: seed the context-bound item projection")
        .new_entity(context);
    first
        .update_customer_order_id(order_id)
        .update_product_id(product_id)
        .update_product_name("Context document product")
        .update_sku("DOC-LINE-1")
        .update_quantity(2)
        .update_commerce_platform_id(platform_id);
    let first_line_id = first.id();
    first
        .audit_as("seed first context-bound OrderLine")
        .save(context)
        .await?;

    let mut second = Q::order_lines()
        .comment("what: construct the second editable generated OrderLine")
        .purpose("why: prove omission differs from explicit removal")
        .new_entity(context);
    second
        .update_customer_order_id(order_id)
        .update_product_id(product_id)
        .update_product_name("Second context document product")
        .update_sku("DOC-LINE-2")
        .update_quantity(9)
        .update_commerce_platform_id(platform_id);
    let second_line_id = second.id();
    second
        .audit_as("seed second context-bound OrderLine")
        .save(context)
        .await?;
    Ok(SeededIds {
        customer_id,
        order_id,
        first_line_id,
        second_line_id,
    })
}

async fn open(context: &UserContext) -> ContextualDocument {
    context
        .open_document(
            DocumentOpenRequest::new(
                "CustomerOrder",
                ORDER_NUMBER,
                PURPOSE,
                Duration::from_secs(900),
            )
            .unwrap(),
        )
        .await
        .unwrap()
}

fn submit(document: &ContextualDocument, command_id: &str, body: Value) -> DocumentAcceptRequest {
    DocumentAcceptRequest::new(
        "CustomerOrder",
        PURPOSE,
        SubmittedContextualDocument {
            business_id: document.business_id.clone(),
            document_token: document.document_token.clone(),
            command_id: command_id.to_owned(),
            body,
        },
    )
    .unwrap()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("TEAQL_CONTEXT_DOCUMENT_DATABASE")?;
    let database_path = database_url
        .strip_prefix("sqlite:file:")
        .ok_or("TEAQL_CONTEXT_DOCUMENT_DATABASE must be a sqlite:file: URL")?;
    let seed_context = service_runtime(ServiceRuntimeConfig {
        database_url: database_url.clone(),
    })
    .await?;
    let ids = seed(&seed_context).await?;
    let permissions = Arc::new(Mutex::new(BTreeSet::from([
        "alice".to_owned(),
        "bob".to_owned(),
    ])));
    let service = Arc::new(GeneratedOrderDocumentService {
        permissions: permissions.clone(),
    });
    let alice = application_context(
        &database_url,
        "alice",
        ids.customer_id,
        permissions.clone(),
        service.clone(),
    )
    .await?;
    let bob = application_context(
        &database_url,
        "bob",
        ids.customer_id,
        permissions.clone(),
        service.clone(),
    )
    .await?;
    let original = open(&alice).await;
    assert_eq!(original.body["items"].as_array().unwrap().len(), 2);
    assert_eq!(
        bob.accept_document(submit(&original, "cross-actor", original.body.clone()))
            .await
            .unwrap_err()
            .code(),
        "ROUND_TRIP_REFERENCE_SCOPE_MISMATCH"
    );

    let restarted = application_context(
        &database_url,
        "alice",
        ids.customer_id,
        permissions.clone(),
        service,
    )
    .await?;
    restarted.consume_document_snapshot(&original.document_token, "CustomerOrder", PURPOSE)?;

    let connection = rusqlite::Connection::open(database_path)?;
    connection.execute_batch(
        "CREATE TRIGGER fail_document_line_update BEFORE UPDATE ON order_line_data \
         BEGIN SELECT RAISE(ABORT, 'injected line write failure'); END;",
    )?;
    let update_body = json!({
        "orderNumber": ORDER_NUMBER,
        "items": [
            {"ref": "item:0", "productName": "Context document product", "sku": "DOC-LINE-1", "quantity": 4},
            {"ref": "item:1", "productName": "Second context document product", "sku": "DOC-LINE-2", "quantity": 9}
        ]
    });
    assert_eq!(
        restarted
            .accept_document(submit(&original, "rollback-retry", update_body.clone()))
            .await
            .unwrap_err()
            .code(),
        "DOCUMENT_VALIDATION_FAILED"
    );
    let rolled_back = Q::customer_orders()
        .with_id_is(ids.order_id)
        .comment("what: reload Order after injected child failure")
        .purpose("why: prove the parent optimistic version rolled back atomically")
        .execute_for_one(&restarted)
        .await?
        .unwrap();
    assert_eq!(
        required(E::customer_order(&rolled_back).get_version().eval())?,
        1
    );
    connection.execute_batch("DROP TRIGGER fail_document_line_update")?;

    let accepted = restarted
        .accept_document(submit(&original, "rollback-retry", update_body))
        .await?;
    assert_eq!((accepted.previous_revision, accepted.new_revision), (1, 2));
    let updated_line = Q::order_lines()
        .with_id_is(ids.first_line_id)
        .comment("what: reload the generated line after document acceptance")
        .purpose("why: prove the Q mutation persisted through SQLite")
        .execute_for_one(&restarted)
        .await?
        .unwrap();
    assert_eq!(
        required(E::order_line(&updated_line).get_quantity().eval())?,
        4
    );

    let mut independently_changed = Q::order_lines()
        .with_id_is(ids.second_line_id)
        .comment("what: load a line for an independent concurrent update")
        .purpose("why: prove child optimistic versions are checked separately")
        .execute_for_one(&restarted)
        .await?
        .unwrap();
    independently_changed.update_quantity(10);
    independently_changed
        .audit_as("simulate a concurrent generated OrderLine update")
        .save(&restarted)
        .await?;
    assert_eq!(
        restarted
            .accept_document(submit(
                &accepted.document,
                "stale-child",
                accepted.document.body.clone()
            ))
            .await
            .unwrap_err()
            .code(),
        "DOCUMENT_REVISION_CONFLICT"
    );

    let fresh = open(&restarted).await;
    let fresh_items = fresh.body["items"].as_array().unwrap();
    let remove_body = json!({
        "orderNumber": ORDER_NUMBER,
        "items": [fresh_items[1].clone()],
        "removedRefs": [fresh_items[0]["ref"].clone()]
    });
    let removed = restarted
        .accept_document(submit(&fresh, "explicit-remove", remove_body))
        .await?;
    assert_eq!(removed.new_revision, 3);
    let remaining = Q::order_lines()
        .with_customer_order_matching(Q::customer_orders_minimal().with_id_is(ids.order_id))
        .order_by_id_asc()
        .limit(10)
        .comment("what: reload active generated lines after explicit removal")
        .purpose("why: prove omission alone did not delete the retained item")
        .execute_for_list(&restarted)
        .await?;
    assert_eq!(remaining.len(), 1);
    assert_eq!(
        required(E::order_line(&remaining[0]).get_id().eval())?,
        ids.second_line_id
    );

    assert_eq!(
        restarted
            .accept_document(submit(&original, "stale-root", original.body.clone()))
            .await
            .unwrap_err()
            .code(),
        "DOCUMENT_REVISION_CONFLICT"
    );
    let revocable = open(&restarted).await;
    permissions.lock().unwrap().remove("alice");
    assert_eq!(
        restarted
            .accept_document(submit(
                &revocable,
                "revoked-before-write",
                revocable.body.clone()
            ))
            .await
            .unwrap_err()
            .code(),
        "ROUND_TRIP_REFERENCE_AUTHORIZATION_REQUIRED"
    );

    println!(
        "CONTEXT_BOUND_SQLITE_QE_PASS order_id={} update_version={} delete_version={} remaining_line_id={}",
        ids.order_id, accepted.new_revision, removed.new_revision, ids.second_line_id
    );
    Ok(())
}
