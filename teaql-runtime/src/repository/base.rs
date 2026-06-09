use teaql_core::{
    BatchInsertCommand, BatchUpdateCommand, DeleteCommand, Entity, InsertCommand, Record,
    RecoverCommand, SelectQuery, SmartList, UpdateCommand,
};
use teaql_data_service::{MutationRequest, QueryRequest};

use crate::{MetadataStore, RepositoryError, RuntimeError};

use super::Repository;

impl<'a, M, E> Repository<'a, M, E>
where
    M: MetadataStore,
    E: teaql_data_service::QueryExecutor + teaql_data_service::MutationExecutor,
{
    pub fn new(metadata: &'a M, executor: &'a E) -> Self {
        Self {
            metadata,
            executor,
        }
    }

    pub async fn fetch_all(&self, query: &SelectQuery) -> Result<Vec<Record>, RepositoryError<E::Error>> {
        let request = QueryRequest {
            query: query.clone(),
            trace_chain: query.trace_chain.clone(),
            comment: query.comment.clone(),
        };
        let res = self.executor.query(request).await.map_err(RepositoryError::Executor)?;
        Ok(res.rows)
    }

    pub async fn fetch_smart_list(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<Record>, RepositoryError<E::Error>> {
        let request = QueryRequest {
            query: query.clone(),
            trace_chain: query.trace_chain.clone(),
            comment: query.comment.clone(),
        };
        let res = self.executor.query(request).await.map_err(RepositoryError::Executor)?;
        self.metadata.record_metadata_log(&res.metadata);
        Ok(SmartList::from(res.rows))
    }

    pub async fn fetch_entities<T>(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<T>, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        self.fetch_all(query).await?
            .into_iter()
            .map(T::from_record)
            .collect::<Result<Vec<_>, _>>()
            .map(SmartList::from)
            .map_err(RepositoryError::Entity)
    }

    pub async fn fetch_enhanced_entities<T>(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<T>, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        self.fetch_entities(query).await
    }

    pub async fn insert(&self, command: &InsertCommand) -> Result<u64, RepositoryError<E::Error>> {
        let request = MutationRequest::Insert(command.clone());
        let res = self.executor.mutate(request).await.map_err(RepositoryError::Executor)?;
        self.metadata.record_metadata_log(&res.metadata);
        Ok(res.affected_rows)
    }

    pub async fn update(&self, command: &UpdateCommand) -> Result<u64, RepositoryError<E::Error>> {
        let request = MutationRequest::Update(command.clone());
        let res = self.executor.mutate(request).await.map_err(RepositoryError::Executor)?;
        self.metadata.record_metadata_log(&res.metadata);
        let affected = res.affected_rows;

        if command.expected_version.is_some() && affected == 0 {
            println!("OptimisticLockConflict in base.rs update! entity={}, id={:?}", command.entity, command.id);
            println!("Backtrace: {:#?}", std::backtrace::Backtrace::force_capture());
            return Err(RepositoryError::Runtime(
                RuntimeError::OptimisticLockConflict {
                    entity: command.entity.clone(),
                    id: format!("{:?}", command.id),
                },
            ));
        }

        Ok(affected)
    }

    pub async fn delete(&self, command: &DeleteCommand) -> Result<u64, RepositoryError<E::Error>> {
        let request = MutationRequest::Delete(command.clone());
        let res = self.executor.mutate(request).await.map_err(RepositoryError::Executor)?;
        self.metadata.record_metadata_log(&res.metadata);
        let affected = res.affected_rows;

        if command.expected_version.is_some() && affected == 0 {
            return Err(RepositoryError::Runtime(
                RuntimeError::OptimisticLockConflict {
                    entity: command.entity.clone(),
                    id: format!("{:?}", command.id),
                },
            ));
        }

        Ok(affected)
    }

    pub async fn batch_insert(
        &self,
        command: &teaql_core::BatchInsertCommand,
    ) -> Result<u64, RepositoryError<E::Error>> {
        // Build individual InsertCommands for now, or use BatchMutation if appropriate
        let mut affected = 0;
        for (i, val) in command.batch_values.iter().enumerate() {
            let mut insert_cmd = InsertCommand::new(command.entity.clone());
            insert_cmd.values = val.clone();
            if i < command.trace_chains.len() {
                insert_cmd.trace_chain = command.trace_chains[i].clone();
            }
            let res = self.executor.mutate(MutationRequest::Insert(insert_cmd)).await.map_err(RepositoryError::Executor)?;
            self.metadata.record_metadata_log(&res.metadata);
            affected += res.affected_rows;
        }
        Ok(affected)
    }

    pub async fn batch_update(
        &self,
        command: &teaql_core::BatchUpdateCommand,
    ) -> Result<u64, RepositoryError<E::Error>> {
        let mut affected = 0;
        for (i, val) in command.batch_values.iter().enumerate() {
            let mut update_cmd = UpdateCommand::new(command.entity.clone(), command.batch_ids[i].clone());
            
            let mut filtered_values = Record::new();
            for field in &command.update_fields {
                if let Some(v) = val.get(field) {
                    filtered_values.insert(field.clone(), v.clone());
                }
            }
            update_cmd.values = filtered_values;
            if let Some(Some(v)) = command.batch_expected_versions.get(i) {
                update_cmd.expected_version = Some(*v);
            }
            if let Some(old) = command.batch_old_values.get(i) {
                update_cmd.old_values = old.clone();
            }
            if i < command.trace_chains.len() {
                update_cmd.trace_chain = command.trace_chains[i].clone();
            }
            let res = self.executor.mutate(MutationRequest::Update(update_cmd)).await.map_err(RepositoryError::Executor)?;
            self.metadata.record_metadata_log(&res.metadata);
            affected += res.affected_rows;
        }

        if command.batch_expected_versions.iter().any(|v| v.is_some()) {
            if affected != command.batch_ids.len() as u64 {
                println!("OptimisticLockConflict in batch_update! entity={}, affected={}, expected={}", command.entity, affected, command.batch_ids.len());
                return Err(RepositoryError::Runtime(
                    RuntimeError::OptimisticLockConflict {
                        entity: command.entity.clone(),
                        id: "BATCH".to_owned(),
                    },
                ));
            }
        }

        Ok(affected)
    }

    pub async fn recover(&self, command: &RecoverCommand) -> Result<u64, RepositoryError<E::Error>> {
        let request = MutationRequest::Recover(command.clone());
        let res = self.executor.mutate(request).await.map_err(RepositoryError::Executor)?;
        self.metadata.record_metadata_log(&res.metadata);
        let affected = res.affected_rows;

        if affected == 0 {
            return Err(RepositoryError::Runtime(
                RuntimeError::OptimisticLockConflict {
                    entity: command.entity.clone(),
                    id: format!("{:?}", command.id),
                },
            ));
        }

        Ok(affected)
    }

    pub async fn insert_many(
        &self,
        commands: &[InsertCommand],
    ) -> Result<u64, RepositoryError<E::Error>> {
        let mut total = 0;
        for command in commands {
            total += self.insert(command).await?;
        }
        Ok(total)
    }

    pub async fn update_many(
        &self,
        commands: &[UpdateCommand],
    ) -> Result<u64, RepositoryError<E::Error>> {
        let mut total = 0;
        for command in commands {
            total += self.update(command).await?;
        }
        Ok(total)
    }

    pub async fn delete_many(
        &self,
        commands: &[DeleteCommand],
    ) -> Result<u64, RepositoryError<E::Error>> {
        let mut total = 0;
        for command in commands {
            total += self.delete(command).await?;
        }
        Ok(total)
    }

    pub async fn recover_many(
        &self,
        commands: &[RecoverCommand],
    ) -> Result<u64, RepositoryError<E::Error>> {
        let mut total = 0;
        for command in commands {
            total += self.recover(command).await?;
        }
        Ok(total)
    }
}
