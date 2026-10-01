use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use teaql_runtime::{
    AcceptedContextualDocument, ContextBoundDocumentService, ContextBoundReferenceRuntime,
    ContextualDocument, DeploymentProfile, DocumentAcceptRequest, DocumentEntitySnapshot,
    DocumentMutationKind, DocumentOpenRequest, DocumentRelationCompleteness,
    DocumentRelationSnapshot, DocumentRoundTripError, DocumentSnapshot, ReferenceDocumentScope,
    ReferenceIdentity, ReferenceKey, RoundTripReferenceError, StaticReferenceKeyProvider,
    SubmittedContextualDocument, TrustedReferencePrincipal, UserContext,
};

const PURPOSE: &str = "edit-order-items";
const MODEL_FINGERPRINT: &str = "order-items-poc-v1";

#[derive(Clone, Debug, PartialEq, Eq)]
struct OrderItem {
    id: u64,
    version: i64,
    product_code: String,
    quantity: i64,
    visible_to: BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Order {
    id: u64,
    revision: i64,
    business_id: String,
    merchant_id: u64,
    items: Vec<OrderItem>,
}

#[derive(Default)]
struct OrderState {
    orders: BTreeMap<String, Order>,
    command_results: BTreeMap<String, AcceptedContextualDocument>,
    next_item_id: u64,
}

#[derive(Clone)]
struct OrderDocumentService {
    state: Arc<Mutex<OrderState>>,
    permissions: Arc<Mutex<BTreeSet<String>>>,
}

impl OrderDocumentService {
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

    fn render(
        &self,
        context: &UserContext,
        principal: &TrustedReferencePrincipal,
        order: &Order,
        lifetime: Duration,
    ) -> Result<ContextualDocument, DocumentRoundTripError> {
        self.require_access(principal)?;
        if principal.domain_root_type != "Merchant" || principal.domain_root_id != order.merchant_id
        {
            return Err(DocumentRoundTripError::authorization_required());
        }
        let document_id = format!("order-{}-revision-{}", order.id, order.revision);
        let scope =
            ReferenceDocumentScope::new(document_id, PURPOSE, "Order", order.id, order.revision)?;
        let mut snapshots = Vec::new();
        let mut visible_items = Vec::new();
        let mut row_keys = Vec::new();
        for (index, item) in order
            .items
            .iter()
            .filter(|item| item.visible_to.contains(&principal.subject))
            .enumerate()
        {
            let row_key = format!("item:{index}");
            row_keys.push(row_key.clone());
            snapshots.push(DocumentEntitySnapshot::new(
                row_key.clone(),
                ReferenceIdentity::new("OrderItem", item.id, item.version)?,
                ["product_code", "quantity"],
                ["quantity"],
                [
                    DocumentMutationKind::Update,
                    DocumentMutationKind::RemoveChild,
                ],
            )?);
            visible_items.push(json!({
                "ref": row_key,
                "productCode": item.product_code,
                "quantity": item.quantity
            }));
        }
        let snapshot = DocumentSnapshot::new(
            MODEL_FINGERPRINT,
            order.business_id.clone(),
            scope,
            ReferenceIdentity::new("Order", order.id, order.revision)?,
            snapshots,
            vec![DocumentRelationSnapshot::new(
                "items",
                row_keys,
                DocumentRelationCompleteness::Complete,
                true,
                [
                    DocumentMutationKind::CreateChild,
                    DocumentMutationKind::RemoveChild,
                ],
            )?],
        )?;
        Ok(ContextualDocument {
            business_id: order.business_id.clone(),
            document_token: context.issue_document_snapshot(snapshot, lifetime)?,
            body: json!({
                "orderNumber": order.business_id,
                "items": visible_items
            }),
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
impl ContextBoundDocumentService for OrderDocumentService {
    async fn open_document(
        &self,
        context: &UserContext,
        principal: &TrustedReferencePrincipal,
        request: DocumentOpenRequest,
    ) -> Result<ContextualDocument, DocumentRoundTripError> {
        self.require_access(principal)?;
        if request.aggregate_type != "Order" || request.purpose != PURPOSE {
            return Err(DocumentRoundTripError::scope_mismatch());
        }
        let state = self.state.lock().unwrap();
        let order = state
            .orders
            .get(&request.business_id)
            .ok_or_else(DocumentRoundTripError::not_found)?;
        self.render(context, principal, order, request.lifetime)
    }

    async fn accept_document(
        &self,
        context: &UserContext,
        principal: &TrustedReferencePrincipal,
        request: DocumentAcceptRequest,
    ) -> Result<AcceptedContextualDocument, DocumentRoundTripError> {
        self.require_access(principal)?;
        if request.expected_aggregate_type != "Order" || request.purpose != PURPOSE {
            return Err(DocumentRoundTripError::scope_mismatch());
        }
        let verified = context.consume_document_snapshot(
            &request.submitted.document_token,
            &request.expected_aggregate_type,
            &request.purpose,
        )?;
        if request.submitted.business_id != verified.snapshot.business_id {
            return Err(DocumentRoundTripError::scope_mismatch());
        }
        if verified.snapshot.model_fingerprint != MODEL_FINGERPRINT
            || request
                .submitted
                .body
                .get("orderNumber")
                .and_then(Value::as_str)
                != Some(verified.snapshot.business_id.as_str())
        {
            return Err(DocumentRoundTripError::projection_violation());
        }

        let mut state = self.state.lock().unwrap();
        if let Some(result) = state.command_results.get(&request.submitted.command_id) {
            return Ok(result.clone());
        }
        let original = state
            .orders
            .get(&verified.snapshot.business_id)
            .ok_or_else(DocumentRoundTripError::not_found)?
            .clone();
        if original.id != verified.snapshot.aggregate.id
            || original.revision != verified.snapshot.aggregate.version
        {
            return Err(DocumentRoundTripError::revision_conflict());
        }
        let relation = verified
            .snapshot
            .relation("items")
            .ok_or_else(DocumentRoundTripError::projection_violation)?;
        let relation_rows: BTreeSet<&str> = relation.row_keys.iter().map(String::as_str).collect();
        let submitted_rows = Self::submitted_rows(&request.submitted.body)?;
        let removed_refs = Self::removed_refs(&request.submitted.body)?;
        let mut candidate = original.clone();
        let mut changed = BTreeSet::new();
        let mut submitted_refs = BTreeSet::new();

        for row in submitted_rows {
            let object = row
                .as_object()
                .ok_or_else(DocumentRoundTripError::projection_violation)?;
            if let Some(row_key) = object.get("ref").and_then(Value::as_str) {
                if object
                    .keys()
                    .any(|key| !matches!(key.as_str(), "ref" | "productCode" | "quantity"))
                {
                    return Err(DocumentRoundTripError::projection_violation());
                }
                if !submitted_refs.insert(row_key) {
                    return Err(DocumentRoundTripError::projection_violation());
                }
                let issued = verified
                    .snapshot
                    .entity(row_key)
                    .ok_or_else(DocumentRoundTripError::projection_violation)?;
                if !relation_rows.contains(row_key)
                    || issued.identity.entity_type != "OrderItem"
                    || !issued
                        .allowed_mutations
                        .contains(&DocumentMutationKind::Update)
                {
                    return Err(DocumentRoundTripError::projection_violation());
                }
                let item = candidate
                    .items
                    .iter_mut()
                    .find(|item| item.id == issued.identity.id)
                    .ok_or_else(DocumentRoundTripError::scope_mismatch)?;
                if item.version != issued.identity.version {
                    return Err(DocumentRoundTripError::revision_conflict());
                }
                if object.get("productCode").and_then(Value::as_str)
                    != Some(item.product_code.as_str())
                {
                    return Err(DocumentRoundTripError::projection_violation());
                }
                let quantity = object
                    .get("quantity")
                    .and_then(Value::as_i64)
                    .ok_or_else(DocumentRoundTripError::validation_failed)?;
                if quantity <= 0 {
                    return Err(DocumentRoundTripError::validation_failed());
                }
                if quantity != item.quantity {
                    item.quantity = quantity;
                    item.version += 1;
                    changed.insert(item.id);
                }
            } else {
                if !relation
                    .allowed_mutations
                    .contains(&DocumentMutationKind::CreateChild)
                {
                    return Err(DocumentRoundTripError::projection_violation());
                }
                if object
                    .keys()
                    .any(|key| !matches!(key.as_str(), "clientKey" | "productCode" | "quantity"))
                {
                    return Err(DocumentRoundTripError::projection_violation());
                }
                let client_key = object
                    .get("clientKey")
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(DocumentRoundTripError::projection_violation)?;
                if !submitted_refs.insert(client_key) {
                    return Err(DocumentRoundTripError::projection_violation());
                }
                let product_code = object
                    .get("productCode")
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(DocumentRoundTripError::validation_failed)?;
                let quantity = object
                    .get("quantity")
                    .and_then(Value::as_i64)
                    .filter(|value| *value > 0)
                    .ok_or_else(DocumentRoundTripError::validation_failed)?;
                state.next_item_id += 1;
                let id = state.next_item_id;
                candidate.items.push(OrderItem {
                    id,
                    version: 1,
                    product_code: product_code.to_owned(),
                    quantity,
                    visible_to: BTreeSet::from([principal.subject.clone()]),
                });
                changed.insert(id);
            }
        }

        let mut unique_removed_refs = BTreeSet::new();
        for row_key in removed_refs {
            if relation.completeness != DocumentRelationCompleteness::Complete
                || !relation.writable
                || !relation_rows.contains(row_key)
                || submitted_refs.contains(row_key)
                || !unique_removed_refs.insert(row_key)
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
            if !issued
                .allowed_mutations
                .contains(&DocumentMutationKind::RemoveChild)
            {
                return Err(DocumentRoundTripError::projection_violation());
            }
            let position = candidate
                .items
                .iter()
                .position(|item| item.id == issued.identity.id)
                .ok_or_else(DocumentRoundTripError::scope_mismatch)?;
            if candidate.items[position].version != issued.identity.version {
                return Err(DocumentRoundTripError::revision_conflict());
            }
            changed.insert(candidate.items[position].id);
            candidate.items.remove(position);
        }

        let previous_revision = candidate.revision;
        if !changed.is_empty() {
            candidate.revision += 1;
        }
        let changed_entities = changed
            .iter()
            .map(|id| {
                candidate
                    .items
                    .iter()
                    .find(|item| item.id == *id)
                    .map(|item| ReferenceIdentity::new("OrderItem", item.id, item.version).unwrap())
                    .unwrap_or_else(|| ReferenceIdentity::new("OrderItem", *id, 0).unwrap())
            })
            .collect();
        state
            .orders
            .insert(candidate.business_id.clone(), candidate.clone());
        let document = self.render(context, principal, &candidate, Duration::from_secs(900))?;
        let result = AcceptedContextualDocument {
            document,
            previous_revision,
            new_revision: candidate.revision,
            changed_entities,
        };
        state
            .command_results
            .insert(request.submitted.command_id, result.clone());
        Ok(result)
    }
}

fn allow_identity(
    permissions: Arc<Mutex<BTreeSet<String>>>,
) -> impl Fn(
    &TrustedReferencePrincipal,
    &ReferenceDocumentScope,
    &ReferenceIdentity,
) -> Result<(), RoundTripReferenceError>
+ Send
+ Sync
+ 'static {
    move |principal, scope, _| {
        if permissions.lock().unwrap().contains(&principal.subject)
            && principal.domain_root_type == "Merchant"
            && principal.domain_root_id == 7
            && scope.aggregate_type == "Order"
        {
            Ok(())
        } else {
            Err(RoundTripReferenceError::authorization_required())
        }
    }
}

fn runtime(
    environment: &str,
    permissions: Arc<Mutex<BTreeSet<String>>>,
) -> Arc<ContextBoundReferenceRuntime> {
    Arc::new(
        ContextBoundReferenceRuntime::governed(
            DeploymentProfile::Test,
            "order-service",
            environment,
            Arc::new(
                StaticReferenceKeyProvider::new(
                    "k1",
                    [ReferenceKey::new("k1", [0x71; 32]).unwrap()],
                )
                .unwrap(),
            ),
            Arc::new(allow_identity(permissions)),
        )
        .unwrap(),
    )
}

fn context(
    subject: &str,
    runtime: Arc<ContextBoundReferenceRuntime>,
    service: Arc<OrderDocumentService>,
) -> UserContext {
    UserContext::new()
        .with_round_trip_reference_runtime(runtime)
        .with_trusted_reference_principal(
            TrustedReferencePrincipal::new("oidc", subject, "Merchant", 7).unwrap(),
        )
        .with_context_bound_document_service(service)
}

fn fixture() -> (Arc<OrderDocumentService>, Arc<Mutex<BTreeSet<String>>>) {
    let permissions = Arc::new(Mutex::new(BTreeSet::from([
        "alice".to_owned(),
        "bob".to_owned(),
    ])));
    let state = OrderState {
        orders: BTreeMap::from([
            (
                "ORD-1001".to_owned(),
                Order {
                    id: 1001,
                    revision: 3,
                    business_id: "ORD-1001".to_owned(),
                    merchant_id: 7,
                    items: vec![
                        OrderItem {
                            id: 11,
                            version: 2,
                            product_code: "VISIBLE".to_owned(),
                            quantity: 2,
                            visible_to: BTreeSet::from(["alice".to_owned(), "bob".to_owned()]),
                        },
                        OrderItem {
                            id: 12,
                            version: 5,
                            product_code: "ALICE-ONLY".to_owned(),
                            quantity: 9,
                            visible_to: BTreeSet::from(["alice".to_owned()]),
                        },
                    ],
                },
            ),
            (
                "ORD-2002".to_owned(),
                Order {
                    id: 2002,
                    revision: 1,
                    business_id: "ORD-2002".to_owned(),
                    merchant_id: 7,
                    items: vec![OrderItem {
                        id: 21,
                        version: 1,
                        product_code: "OTHER-ORDER".to_owned(),
                        quantity: 1,
                        visible_to: BTreeSet::from(["alice".to_owned()]),
                    }],
                },
            ),
        ]),
        command_results: BTreeMap::new(),
        next_item_id: 100,
    };
    (
        Arc::new(OrderDocumentService {
            state: Arc::new(Mutex::new(state)),
            permissions: permissions.clone(),
        }),
        permissions,
    )
}

async fn open(context: &UserContext, business_id: &str) -> ContextualDocument {
    context
        .open_document(
            DocumentOpenRequest::new("Order", business_id, PURPOSE, Duration::from_secs(900))
                .unwrap(),
        )
        .await
        .unwrap()
}

fn submit(document: &ContextualDocument, command_id: &str, body: Value) -> DocumentAcceptRequest {
    DocumentAcceptRequest::new(
        "Order",
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

#[tokio::test]
async fn order_document_round_trip_is_context_bound_and_projection_safe() {
    let (service, permissions) = fixture();
    let shared_runtime = runtime("test", permissions.clone());
    let alice = context("alice", shared_runtime.clone(), service.clone());
    let bob = context("bob", shared_runtime, service.clone());

    let alice_document = open(&alice, "ORD-1001").await;
    let bob_document = open(&bob, "ORD-1001").await;
    assert_ne!(alice_document.document_token, bob_document.document_token);
    assert_eq!(alice_document.body["items"].as_array().unwrap().len(), 2);
    assert_eq!(bob_document.body["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        bob.accept_document(submit(
            &alice_document,
            "bob-steal",
            alice_document.body.clone()
        ))
        .await
        .unwrap_err()
        .code(),
        "ROUND_TRIP_REFERENCE_SCOPE_MISMATCH"
    );

    let invalid_body = json!({
        "orderNumber": "ORD-1001",
        "items": [{"ref": "item:0", "productCode": "VISIBLE", "quantity": 0}]
    });
    assert_eq!(
        alice
            .accept_document(submit(&alice_document, "validation-1", invalid_body))
            .await
            .unwrap_err()
            .code(),
        "DOCUMENT_VALIDATION_FAILED"
    );
    assert_eq!(service.state.lock().unwrap().orders["ORD-1001"].revision, 3);

    let corrected_body = json!({
        "orderNumber": "ORD-1001",
        "items": [{"ref": "item:0", "productCode": "VISIBLE", "quantity": 4}]
    });
    let accepted = alice
        .accept_document(submit(&alice_document, "accept-1", corrected_body))
        .await
        .unwrap();
    assert_eq!((accepted.previous_revision, accepted.new_revision), (3, 4));
    assert_ne!(
        accepted.document.document_token,
        alice_document.document_token
    );
    let stored = service.state.lock().unwrap().orders["ORD-1001"].clone();
    assert_eq!(stored.items.len(), 2, "omitted invisible row must survive");
    assert_eq!(stored.items[0].quantity, 4);
    assert_eq!(stored.items[1].product_code, "ALICE-ONLY");

    let remove_body = json!({
        "orderNumber": "ORD-1001",
        "items": [{"ref": "item:1", "productCode": "ALICE-ONLY", "quantity": 9}],
        "removedRefs": ["item:0"]
    });
    let removed = alice
        .accept_document(submit(&accepted.document, "remove-1", remove_body))
        .await
        .unwrap();
    assert_eq!(removed.new_revision, 5);
    let stored_after_remove = service.state.lock().unwrap().orders["ORD-1001"].clone();
    assert_eq!(stored_after_remove.items.len(), 1);
    assert_eq!(stored_after_remove.items[0].product_code, "ALICE-ONLY");

    assert_eq!(
        alice
            .accept_document(submit(
                &alice_document,
                "stale-1",
                alice_document.body.clone()
            ))
            .await
            .unwrap_err()
            .code(),
        "DOCUMENT_REVISION_CONFLICT"
    );

    permissions.lock().unwrap().remove("alice");
    assert_eq!(
        alice
            .accept_document(submit(
                &accepted.document,
                "revoked-1",
                accepted.document.body.clone()
            ))
            .await
            .unwrap_err()
            .code(),
        "ROUND_TRIP_REFERENCE_AUTHORIZATION_REQUIRED"
    );
}

#[tokio::test]
async fn token_survives_restart_but_not_environment_or_cross_order_substitution() {
    let (service, permissions) = fixture();
    let first = context(
        "alice",
        runtime("test", permissions.clone()),
        service.clone(),
    );
    let document = open(&first, "ORD-1001").await;

    let restarted = context(
        "alice",
        runtime("test", permissions.clone()),
        service.clone(),
    );
    restarted
        .consume_document_snapshot(&document.document_token, "Order", PURPOSE)
        .unwrap();
    let production = context("alice", runtime("production", permissions), service.clone());
    assert_eq!(
        production
            .consume_document_snapshot(&document.document_token, "Order", PURPOSE)
            .unwrap_err()
            .code(),
        "ROUND_TRIP_REFERENCE_INVALID"
    );

    let other = open(&restarted, "ORD-2002").await;
    let substituted = SubmittedContextualDocument {
        business_id: document.business_id.clone(),
        document_token: other.document_token.clone(),
        command_id: "cross-order".to_owned(),
        body: other.body.clone(),
    };
    assert_eq!(
        restarted
            .accept_document(DocumentAcceptRequest::new("Order", PURPOSE, substituted).unwrap())
            .await
            .unwrap_err()
            .code(),
        "ROUND_TRIP_REFERENCE_SCOPE_MISMATCH"
    );
}

#[tokio::test]
async fn child_version_conflict_is_detected_even_if_aggregate_revision_is_unchanged() {
    let (service, permissions) = fixture();
    let context = context("alice", runtime("test", permissions), service.clone());
    let document = open(&context, "ORD-1001").await;
    service
        .state
        .lock()
        .unwrap()
        .orders
        .get_mut("ORD-1001")
        .unwrap()
        .items[0]
        .version += 1;
    assert_eq!(
        context
            .accept_document(submit(&document, "child-stale", document.body.clone()))
            .await
            .unwrap_err()
            .code(),
        "DOCUMENT_REVISION_CONFLICT"
    );
}
