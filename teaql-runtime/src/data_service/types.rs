use teaql_core::{EntityDescriptor, SelectQuery};

use crate::{MetadataStore, UserContext};

pub struct RuntimeDataService<'a, M, E> {
    pub(super) metadata: &'a M,
    pub(super) executor: &'a E,
}

pub struct ContextDataService<'a, E> {
    pub(super) metadata: UserContextMetadata<'a>,
    pub(crate) executor: &'a E,
}

pub struct EntityDataService<'a, E> {
    pub(super) entity: String,
    pub(super) data_service: ContextDataService<'a, E>,
    pub(super) trace_context: Vec<teaql_core::TraceNode>,
}

impl<'a, E> EntityDataService<'a, E> {
    pub fn with_trace_context(mut self, trace_context: Vec<teaql_core::TraceNode>) -> Self {
        self.trace_context = trace_context;
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelationLoadPlan {
    pub parent_entity: String,
    pub relation_name: String,
    pub path: String,
    pub target_entity: String,
    pub local_key: String,
    pub foreign_key: String,
    pub many: bool,
    pub query: Option<SelectQuery>,
    pub children: Vec<RelationLoadPlan>,
}

pub(crate) struct UserContextMetadata<'a> {
    pub(crate) context: &'a UserContext,
}

impl MetadataStore for UserContextMetadata<'_> {
    fn entity(&self, name: &str) -> Option<&EntityDescriptor> {
        self.context.entity(name)
    }

    fn all_entities(&self) -> Vec<&EntityDescriptor> {
        self.context
            .metadata
            .as_ref()
            .map(|metadata| metadata.all_entities())
            .unwrap_or_default()
    }

    fn record_metadata_log(&self, metadata: &teaql_data_service::ExecutionMetadata) {
        self.context.record_metadata_log(metadata);
    }
}
