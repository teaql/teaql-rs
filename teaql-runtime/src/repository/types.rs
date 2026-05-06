use teaql_core::{EntityDescriptor, SelectQuery};

use crate::{MetadataStore, UserContext};

pub struct Repository<'a, D, M, E> {
    pub(super) dialect: &'a D,
    pub(super) metadata: &'a M,
    pub(super) executor: &'a E,
}

pub struct ContextRepository<'a, D, E> {
    pub(super) metadata: UserContextMetadata<'a>,
    pub(crate) dialect: &'a D,
    pub(crate) executor: &'a E,
}

pub struct ResolvedRepository<'a, D, E> {
    pub(super) entity: String,
    pub(super) repository: ContextRepository<'a, D, E>,
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
}
