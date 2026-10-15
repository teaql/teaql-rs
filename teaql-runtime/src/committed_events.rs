//! Source-side committed-event configuration and fact freezing.
//!
//! The Entity Mutation Ledger is an intent, not a durable event. Only a
//! transaction-owned provider readback may become a committed mutation fact.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value as JsonValue, json};
use teaql_core::{CompactRow, DataType, EntityDescriptor, TraceKind, Value};
use teaql_data_service::{CommittedSetAppend, MutationExecutor, QueryExecutor};

use crate::{
    DataServiceError, EntityDataService, EntityKey, EntityRuntimeState, RuntimeError, UserContext,
};

/// Trusted startup identity of one ordered source stream. The model hash is
/// currently supplied by the application; derived-model validation is a
/// separate acceptance gate.
#[derive(Clone)]
pub struct CommittedEventSource {
    pub source_runtime_id: String,
    pub model_hash: String,
}

impl std::fmt::Debug for CommittedEventSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CommittedEventSource")
            .field("source_runtime_id", &self.source_runtime_id)
            .field("model_hash", &self.model_hash)
            .finish()
    }
}

impl CommittedEventSource {
    pub fn new(
        source_runtime_id: impl Into<String>,
        model_hash: impl Into<String>,
    ) -> Result<Self, RuntimeError> {
        let source_runtime_id = source_runtime_id.into();
        let model_hash = model_hash.into();
        let valid_hash = model_hash.strip_prefix("sha256:").is_some_and(|hex| {
            hex.len() == 64
                && hex
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        });
        if source_runtime_id.trim().is_empty() || !valid_hash {
            return Err(RuntimeError::Graph(
                "committed-event source and SHA-256 model hash are required".into(),
            ));
        }
        Ok(Self {
            source_runtime_id,
            model_hash,
        })
    }
}

pub(crate) async fn capture_update_preimages<E>(
    context: &UserContext,
    service: &EntityDataService<'_, E>,
    root: &EntityRuntimeState,
) -> Result<BTreeMap<EntityKey, CompactRow>, DataServiceError<E::Error>>
where
    E: QueryExecutor + MutationExecutor + Send + Sync,
{
    let new = root.new_keys();
    let deleted = root.deleted_keys();
    let mut rows = BTreeMap::new();
    for key in root.current_change_set().changes().keys() {
        if new.contains(key) || deleted.contains(key) {
            continue;
        }
        let descriptor = context
            .require_entity(&key.entity)
            .map_err(DataServiceError::Runtime)?;
        let id_property = descriptor.id_property().ok_or_else(|| {
            DataServiceError::Runtime(RuntimeError::Graph(format!(
                "entity {} has no ID property",
                key.entity
            )))
        })?;
        let row = service
            .fetch_graph_current_row_internal(
                &key.entity,
                &id_property.name,
                &key.id,
                root.get_trace_chain(key),
            )
            .await?
            .ok_or_else(|| {
                DataServiceError::Runtime(RuntimeError::Graph(format!(
                    "pre-commit {} row could not be read inside its transaction",
                    key.entity
                )))
            })?;
        rows.insert(key.clone(), row);
    }
    Ok(rows)
}

pub(crate) async fn freeze_ledger_set<E>(
    context: &UserContext,
    service: &EntityDataService<'_, E>,
    root: &EntityRuntimeState,
    generated_ids: &BTreeMap<EntityKey, Value>,
    update_preimages: &BTreeMap<EntityKey, CompactRow>,
    source: &CommittedEventSource,
) -> Result<Option<CommittedSetAppend>, DataServiceError<E::Error>>
where
    E: QueryExecutor + MutationExecutor + Send + Sync,
{
    if root.has_pending_dynamic_mutations() {
        return Err(DataServiceError::Runtime(RuntimeError::Graph(
            "committed-event capture cannot yet encode dynamic field changes".into(),
        )));
    }
    let changes = root.current_change_set();
    let deleted = root.deleted_keys();
    let new = root.new_keys();
    let mut affected = BTreeSet::new();
    affected.extend(changes.changes().keys().cloned());
    affected.extend(deleted.iter().cloned());
    affected.extend(new.iter().cloned());
    let mut facts = Vec::new();
    for key in affected {
        if new.contains(&key) && deleted.contains(&key) {
            continue;
        }
        let descriptor = context
            .require_entity(&key.entity)
            .map_err(DataServiceError::Runtime)?;
        let id_property = descriptor.id_property().ok_or_else(|| {
            DataServiceError::Runtime(RuntimeError::Graph(format!(
                "entity {} has no ID property",
                key.entity
            )))
        })?;
        let version_property = descriptor.version_property().ok_or_else(|| {
            DataServiceError::Runtime(RuntimeError::Graph(format!(
                "entity {} has no version property",
                key.entity
            )))
        })?;
        let id = generated_ids.get(&key).unwrap_or(&key.id);
        let id_text = id
            .try_u64()
            .ok_or_else(|| {
                DataServiceError::Runtime(RuntimeError::Graph(format!(
                    "committed {} identity is not an unsigned integer",
                    key.entity
                )))
            })?
            .to_string();
        let row = service
            .fetch_graph_current_row_internal(
                &key.entity,
                &id_property.name,
                id,
                root.get_trace_chain(&key),
            )
            .await?
            .ok_or_else(|| {
                DataServiceError::Runtime(RuntimeError::Graph(format!(
                    "committed {} row could not be read back inside its transaction",
                    key.entity
                )))
            })?;
        let version = row
            .get(&version_property.name)
            .and_then(Value::try_i64)
            .ok_or_else(|| {
                DataServiceError::Runtime(RuntimeError::Graph(format!(
                    "committed {} version is missing",
                    key.entity
                )))
            })?;
        let is_create = new.contains(&key);
        let is_delete = deleted.contains(&key);
        let base_version = if is_create {
            None
        } else {
            Some(
                root.get_original_version(&key)
                    .ok_or_else(|| {
                        DataServiceError::Runtime(RuntimeError::Graph(format!(
                            "committed {} predecessor version is missing",
                            key.entity
                        )))
                    })?
                    .to_string(),
            )
        };
        let mut fields = serde_json::Map::new();
        if !is_delete {
            for property in &descriptor.properties {
                if property.is_id || property.is_version {
                    continue;
                }
                let value = row.get(&property.name).ok_or_else(|| {
                    DataServiceError::Runtime(RuntimeError::Graph(format!(
                        "committed {}.{} field was not read back",
                        key.entity, property.name
                    )))
                })?;
                let committed_value =
                    field_json(descriptor, &property.name, property.data_type, value)
                        .map_err(DataServiceError::Runtime)?;
                if !is_create {
                    let prior_row = update_preimages.get(&key).ok_or_else(|| {
                        DataServiceError::Runtime(RuntimeError::Graph(format!(
                            "committed {} update has no transaction preimage",
                            key.entity
                        )))
                    })?;
                    let prior_value = prior_row.get(&property.name).ok_or_else(|| {
                        DataServiceError::Runtime(RuntimeError::Graph(format!(
                            "committed {}.{} preimage field is missing",
                            key.entity, property.name
                        )))
                    })?;
                    let prior_value =
                        field_json(descriptor, &property.name, property.data_type, prior_value)
                            .map_err(DataServiceError::Runtime)?;
                    if prior_value == committed_value {
                        continue;
                    }
                }
                fields.insert(property.name.clone(), committed_value);
            }
        }
        let chain = root
            .get_trace_chain(&key)
            .into_iter()
            .map(|node| {
                let kind = match node.kind {
                    TraceKind::Operation => "OPERATION",
                    TraceKind::Request => "REQUEST",
                    TraceKind::Relation => "RELATION",
                    TraceKind::Entity => "ENTITY",
                    TraceKind::Provider => "PROVIDER",
                    TraceKind::Sql => "SQL",
                    TraceKind::Comment => "COMMENT",
                    TraceKind::Purpose => "PURPOSE",
                    TraceKind::AuditReason => "AUDIT_REASON",
                };
                json!({
                    "kind": kind,
                    "name": node.entity_type,
                    "entity_id": node.entity_id.map(|id| id.to_string()),
                    "comment": node.comment,
                })
            })
            .collect::<Vec<_>>();
        facts.push(json!({
            "entity_type": key.entity,
            "entity_id": id_text,
            "operation": if is_delete { "delete" } else if is_create { "create" } else { "update" },
            "base_version": base_version,
            "committed_version": version.to_string(),
            "fields": fields,
            "trace_chain": chain,
        }));
    }
    if facts.is_empty() {
        return Ok(None);
    }
    let change_set_id = uuid::Uuid::new_v4().to_string();
    let comment = root.get_comment().unwrap_or_default();
    let envelope = json!({
        "protocol_version": 1,
        "change_set_id": change_set_id,
        "source_runtime_id": source.source_runtime_id,
        "source_model_hash": source.model_hash,
        "source_transaction_id": change_set_id,
        "committed_at": chrono::Utc::now().to_rfc3339(),
        "audit": {
            "actor_ref": context.user_identifier(),
            "comment": comment,
            "purpose": "replicate committed graph",
            "trace_id": context.trace_id(),
        },
        "mutations": facts,
    });
    Ok(Some(CommittedSetAppend {
        source_runtime_id: source.source_runtime_id.clone(),
        change_set_id,
        model_hash: source.model_hash.clone(),
        payload_json: envelope.to_string(),
    }))
}

fn field_json(
    descriptor: &EntityDescriptor,
    field: &str,
    data_type: DataType,
    value: &Value,
) -> Result<JsonValue, RuntimeError> {
    if matches!(value, Value::Null | Value::TypedNull(_)) {
        return Ok(JsonValue::Null);
    }
    if let Some(relation) = descriptor
        .relations
        .iter()
        .find(|relation| !relation.many && relation.local_key == field)
    {
        return value
            .try_u64()
            .map(|id| {
                json!({
                    "entity_type": relation.target_entity,
                    "entity_id": id.to_string(),
                })
            })
            .ok_or_else(|| {
                RuntimeError::Graph(format!("committed reference {field} has no identity"))
            });
    }
    match (data_type, value) {
        (DataType::Bool, Value::Bool(value)) => Ok(json!(value)),
        (DataType::I64 | DataType::U64, Value::I64(value)) => Ok(json!(value)),
        (DataType::I64 | DataType::U64, Value::U64(value)) => Ok(json!(value)),
        (DataType::F64, Value::F64(value)) if value.is_finite() => Ok(json!(value)),
        (DataType::Decimal, Value::Decimal(value)) => Ok(json!(value.to_string())),
        (DataType::Text | DataType::LargeText, Value::Text(value)) => Ok(json!(value)),
        (DataType::Json, Value::Json(value)) => Ok(value.clone()),
        (DataType::Date, Value::Date(value)) => Ok(json!(value.to_string())),
        (DataType::Timestamp, Value::Timestamp(value)) => Ok(json!(value.as_millis())),
        _ => Err(RuntimeError::Graph(format!(
            "committed field {field} has unsupported readback type"
        ))),
    }
}
