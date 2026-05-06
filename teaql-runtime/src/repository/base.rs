use teaql_core::{
    DeleteCommand, Entity, InsertCommand, Record, RecoverCommand, SelectQuery, SmartList,
    UpdateCommand,
};
use teaql_sql::{CompiledQuery, SqlDialect};

use crate::{MetadataStore, RepositoryError, RuntimeError};

use super::{QueryExecutor, Repository};

impl<'a, D, M, E> Repository<'a, D, M, E>
where
    D: SqlDialect,
    M: MetadataStore,
    E: QueryExecutor,
{
    pub fn new(dialect: &'a D, metadata: &'a M, executor: &'a E) -> Self {
        Self {
            dialect,
            metadata,
            executor,
        }
    }

    pub fn compile(&self, query: &SelectQuery) -> Result<CompiledQuery, RuntimeError> {
        let entity = self
            .metadata
            .entity(&query.entity)
            .ok_or_else(|| RuntimeError::MissingEntity(query.entity.clone()))?;
        Ok(self.dialect.compile_select(entity, query)?)
    }

    pub fn compile_insert(&self, command: &InsertCommand) -> Result<CompiledQuery, RuntimeError> {
        let entity = self
            .metadata
            .entity(&command.entity)
            .ok_or_else(|| RuntimeError::MissingEntity(command.entity.clone()))?;
        Ok(self.dialect.compile_insert(entity, command)?)
    }

    pub fn compile_update(&self, command: &UpdateCommand) -> Result<CompiledQuery, RuntimeError> {
        let entity = self
            .metadata
            .entity(&command.entity)
            .ok_or_else(|| RuntimeError::MissingEntity(command.entity.clone()))?;
        Ok(self.dialect.compile_update(entity, command)?)
    }

    pub fn compile_delete(&self, command: &DeleteCommand) -> Result<CompiledQuery, RuntimeError> {
        let entity = self
            .metadata
            .entity(&command.entity)
            .ok_or_else(|| RuntimeError::MissingEntity(command.entity.clone()))?;
        Ok(self.dialect.compile_delete(entity, command)?)
    }

    pub fn compile_recover(&self, command: &RecoverCommand) -> Result<CompiledQuery, RuntimeError> {
        let entity = self
            .metadata
            .entity(&command.entity)
            .ok_or_else(|| RuntimeError::MissingEntity(command.entity.clone()))?;
        Ok(self.dialect.compile_recover(entity, command)?)
    }

    pub fn fetch_all(&self, query: &SelectQuery) -> Result<Vec<Record>, RepositoryError<E::Error>> {
        let compiled = self.compile(query).map_err(RepositoryError::Runtime)?;
        self.executor
            .fetch_all(&compiled)
            .map_err(RepositoryError::Executor)
    }

    pub fn fetch_smart_list(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<Record>, RepositoryError<E::Error>> {
        self.fetch_all(query).map(SmartList::from)
    }

    pub fn fetch_entities<T>(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<T>, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        self.fetch_all(query)?
            .into_iter()
            .map(T::from_record)
            .collect::<Result<Vec<_>, _>>()
            .map(SmartList::from)
            .map_err(RepositoryError::Entity)
    }

    pub fn fetch_enhanced_entities<T>(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<T>, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        self.fetch_entities(query)
    }

    pub fn insert(&self, command: &InsertCommand) -> Result<u64, RepositoryError<E::Error>> {
        let compiled = self
            .compile_insert(command)
            .map_err(RepositoryError::Runtime)?;
        self.executor
            .execute(&compiled)
            .map_err(RepositoryError::Executor)
    }

    pub fn update(&self, command: &UpdateCommand) -> Result<u64, RepositoryError<E::Error>> {
        let compiled = self
            .compile_update(command)
            .map_err(RepositoryError::Runtime)?;
        let affected = self
            .executor
            .execute(&compiled)
            .map_err(RepositoryError::Executor)?;

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

    pub fn delete(&self, command: &DeleteCommand) -> Result<u64, RepositoryError<E::Error>> {
        let compiled = self
            .compile_delete(command)
            .map_err(RepositoryError::Runtime)?;
        let affected = self
            .executor
            .execute(&compiled)
            .map_err(RepositoryError::Executor)?;

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

    pub fn recover(&self, command: &RecoverCommand) -> Result<u64, RepositoryError<E::Error>> {
        let compiled = self
            .compile_recover(command)
            .map_err(RepositoryError::Runtime)?;
        let affected = self
            .executor
            .execute(&compiled)
            .map_err(RepositoryError::Executor)?;

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

    pub fn insert_many(
        &self,
        commands: &[InsertCommand],
    ) -> Result<u64, RepositoryError<E::Error>> {
        let mut total = 0;
        for command in commands {
            total += self.insert(command)?;
        }
        Ok(total)
    }

    pub fn update_many(
        &self,
        commands: &[UpdateCommand],
    ) -> Result<u64, RepositoryError<E::Error>> {
        let mut total = 0;
        for command in commands {
            total += self.update(command)?;
        }
        Ok(total)
    }

    pub fn delete_many(
        &self,
        commands: &[DeleteCommand],
    ) -> Result<u64, RepositoryError<E::Error>> {
        let mut total = 0;
        for command in commands {
            total += self.delete(command)?;
        }
        Ok(total)
    }

    pub fn recover_many(
        &self,
        commands: &[RecoverCommand],
    ) -> Result<u64, RepositoryError<E::Error>> {
        let mut total = 0;
        for command in commands {
            total += self.recover(command)?;
        }
        Ok(total)
    }
}
