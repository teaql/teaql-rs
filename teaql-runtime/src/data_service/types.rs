use teaql_core::{EntityDescriptor, SelectQuery};

use crate::{MetadataStore, UserContext};

pub(crate) struct RuntimeDataService<'a, M, E> {
    pub(super) metadata: &'a M,
    pub(super) executor: &'a E,
    pub(super) mutation_intent: Option<teaql_core::MutationIntent>,
}

pub(crate) struct ContextDataService<'a, E> {
    pub(super) metadata: UserContextMetadata<'a>,
    pub(crate) executor: &'a E,
    pub(super) mutation_intent: Option<teaql_core::MutationIntent>,
}

pub struct EntityDataService<'a, E> {
    pub(super) entity: String,
    pub(super) data_service: ContextDataService<'a, E>,
    pub(super) trace_context: Vec<teaql_core::TraceNode>,
    /// Private operation scope; never inferred from caller-supplied trace nodes.
    pub(super) request_intent: Option<teaql_core::QueryIntent>,
    // Present only on a private query-tree scope. Never attached to UserContext,
    // the public reusable repository, an entity ledger or a wire request.
    pub(super) query_log_intent: Option<std::sync::Mutex<teaql_data_service::SqlIntentRedactions>>,
}

impl<'a, E> EntityDataService<'a, E> {
    /// Bind one entity data service to an explicit executor.
    ///
    /// Transaction scopes use this constructor to ensure generated repositories
    /// execute against the transaction-owned connection instead of the ambient
    /// context executor.
    pub fn for_executor(
        context: &'a UserContext,
        entity: impl Into<String>,
        executor: &'a E,
    ) -> Self {
        Self {
            entity: entity.into(),
            data_service: ContextDataService {
                metadata: UserContextMetadata { context },
                executor,
                mutation_intent: None,
            },
            trace_context: Vec::new(),
            request_intent: None,
            query_log_intent: None,
        }
    }

    pub fn with_trace_context(mut self, trace_context: Vec<teaql_core::TraceNode>) -> Self {
        self.trace_context = trace_context;
        self
    }

    pub(crate) fn with_mutation_intent(&self, intent: teaql_core::MutationIntent) -> Self
    where
        E: teaql_data_service::QueryExecutor + teaql_data_service::MutationExecutor + Send + Sync,
    {
        let mut scoped = self.scoped_data_service_internal(self.entity.clone());
        scoped.trace_context = self.trace_context.clone();
        scoped.request_intent = Some(
            teaql_core::QueryIntent::new(
                intent.comment(),
                "runtime: load state for an audited graph mutation",
            )
            .expect("validated mutation intent"),
        );
        scoped.data_service.mutation_intent = Some(intent);
        scoped
    }

    pub(super) fn request_intent_for(
        &self,
        query: &SelectQuery,
    ) -> Result<teaql_core::QueryIntent, crate::RuntimeError> {
        if let Some(intent) = &self.request_intent {
            return Ok(intent.clone());
        }
        Ok(teaql_core::QueryIntent::from_optional(
            query.comment.as_deref(),
            query.purpose.as_deref(),
        )?)
    }

    pub(super) fn query_intent_snapshot(&self) -> Option<teaql_data_service::SqlIntentRedactions> {
        self.query_log_intent.as_ref().map(|intent| {
            intent
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        })
    }

    pub(super) fn record_query_metadata(&self, metadata: &teaql_data_service::ExecutionMetadata) {
        let Some(intent) = &self.query_log_intent else {
            self.data_service
                .metadata
                .context
                .record_metadata_log(metadata);
            return;
        };
        let mut metadata = metadata.clone();
        // A provider can return several independently bound statements.
        // Accumulate each statement's normalized provenance for descendants;
        // never interpret one statement's positional policies against another.
        fn inherit(
            metadata: &mut teaql_data_service::ExecutionMetadata,
            inherited: &mut teaql_data_service::SqlIntentRedactions,
        ) {
            if !metadata.statements.is_empty() {
                for statement in &mut metadata.statements {
                    inherit(statement, inherited);
                }
                return;
            }
            metadata.sql_log.intent_redactions.extend(inherited);
            *inherited = teaql_data_service::SqlIntentRedactions::from_bindings(
                &metadata.sql_log,
                &metadata.params,
                metadata.parameterized_query.as_deref().unwrap_or_default(),
            );
        }
        {
            let mut inherited = intent
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            inherit(&mut metadata, &mut inherited);
        }
        // No scope lock survives callbacks; the runtime projects before all sinks.
        self.data_service
            .metadata
            .context
            .record_metadata_log(&metadata);
    }

    pub(super) fn query_diagnostic_observer(
        &self,
    ) -> Option<teaql_data_service::ExecutionObserver<'_>>
    where
        E: Sync,
    {
        self.data_service
            .metadata
            .capture_execution_metadata()
            .then(|| {
                std::sync::Arc::new(move |metadata| self.record_query_metadata(&metadata))
                    as teaql_data_service::ExecutionObserver<'_>
            })
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

    fn mutation_diagnostic_observer(&self) -> Option<teaql_data_service::ExecutionObserver<'_>> {
        self.context.sql_log_options().mutation.then(|| {
            std::sync::Arc::new(move |metadata| self.context.record_metadata_log(&metadata))
                as teaql_data_service::ExecutionObserver<'_>
        })
    }

    fn capture_query_debug(&self) -> bool {
        self.context.sql_log_options().select
    }

    fn capture_execution_metadata(&self) -> bool {
        self.context.sql_log_options().select
    }
}
