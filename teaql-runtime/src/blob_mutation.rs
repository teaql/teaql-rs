//! Framework-neutral, optional binding participant contract. The runtime
//! owns sequencing; the Blob module owns policy and catalog semantics.

use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc};

use teaql_core::Value;
use teaql_data_service::infrastructure_sql::InfrastructureSql;

use crate::{EntityKey, MutationPlan, RuntimeError, UserContext};

pub type BlobMutationFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, RuntimeError>> + Send + 'a>>;

pub trait RuntimeBlobMutationParticipant: Send + Sync {
    fn plan(
        &self,
        context: &UserContext,
        approved: &MutationPlan,
    ) -> Result<Option<Box<dyn RuntimeBlobMutationPlan>>, RuntimeError>;
}

pub trait RuntimeBlobMutationPlan: Send {
    fn prepare<'a>(
        self: Box<Self>,
        context: &'a UserContext,
        sql: &'a dyn InfrastructureSql,
        resolved_ids: &'a BTreeMap<EntityKey, Value>,
    ) -> BlobMutationFuture<'a, Box<dyn RuntimePreparedBlobMutation>>;
}

pub trait RuntimePreparedBlobMutation: Send {
    fn apply<'a>(
        self: Box<Self>,
        context: &'a UserContext,
        sql: &'a dyn InfrastructureSql,
    ) -> BlobMutationFuture<'a, ()>;
}

pub struct BlobMutationParticipantResource(pub Arc<dyn RuntimeBlobMutationParticipant>);
