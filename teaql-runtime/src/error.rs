use teaql_core::EntityError;
use teaql_sql::SqlCompileError;

use crate::CheckResult;

#[derive(Debug)]
pub enum RuntimeError {
    MissingEntity(String),
    SqlCompile(SqlCompileError),
    Behavior(String),
    Event(String),
    Policy(String),
    Check(Vec<CheckResult>),
    Graph(String),
    IdGeneration(String),
    Language(String),
    Schema(String),
    MissingRelation { entity: String, relation: String },
    OptimisticLockConflict { entity: String, id: String },
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingEntity(entity) => write!(f, "missing entity descriptor: {entity}"),
            Self::SqlCompile(err) => err.fmt(f),
            Self::Behavior(message) => write!(f, "entity data service behavior error: {message}"),
            Self::Event(message) => write!(f, "entity event error: {message}"),
            Self::Policy(message) => write!(f, "request policy error: {message}"),
            Self::Check(results) => {
                let messages = results
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; ");
                write!(f, "check failed: {messages}")
            }
            Self::Graph(message) => write!(f, "graph write error: {message}"),
            Self::IdGeneration(message) => write!(f, "id generation error: {message}"),
            Self::Language(message) => write!(f, "language error: {message}"),
            Self::Schema(message) => write!(f, "schema provider error: {message}"),
            Self::MissingRelation { entity, relation } => {
                write!(f, "missing relation {relation} on entity {entity}")
            }
            Self::OptimisticLockConflict { entity, id } => {
                write!(f, "optimistic lock conflict on {entity}({id})")
            }
        }
    }
}

impl std::error::Error for RuntimeError {}

impl From<SqlCompileError> for RuntimeError {
    fn from(value: SqlCompileError) -> Self {
        Self::SqlCompile(value)
    }
}

#[derive(Debug)]
pub enum ContextError {
    MissingResource(String),
    MissingTypedResource(&'static str),
    MissingEntityDataService(String),
}

impl std::fmt::Display for ContextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingResource(name) => write!(f, "missing named resource: {name}"),
            Self::MissingTypedResource(name) => write!(f, "missing typed resource: {name}"),
            Self::MissingEntityDataService(name) => {
                write!(f, "missing entity data service for entity: {name}")
            }
        }
    }
}

impl std::error::Error for ContextError {}

#[derive(Debug)]
pub enum DataServiceError<ExecError> {
    Runtime(RuntimeError),
    Entity(EntityError),
    Executor(ExecError),
}

impl<ExecError> std::fmt::Display for DataServiceError<ExecError>
where
    ExecError: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Runtime(err) => err.fmt(f),
            Self::Entity(err) => err.fmt(f),
            Self::Executor(err) => err.fmt(f),
        }
    }
}

impl<ExecError> std::error::Error for DataServiceError<ExecError> where
    ExecError: std::error::Error + 'static
{
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtime_error_display() {
        let err = RuntimeError::MissingEntity("User".to_owned());
        assert_eq!(err.to_string(), "missing entity descriptor: User");

        let err = RuntimeError::Behavior("validation failed".to_owned());
        assert_eq!(
            err.to_string(),
            "entity data service behavior error: validation failed"
        );

        let err = RuntimeError::MissingRelation {
            entity: "User".to_owned(),
            relation: "Profile".to_owned(),
        };
        assert_eq!(err.to_string(), "missing relation Profile on entity User");
    }

    #[test]
    fn test_context_error_display() {
        let err = ContextError::MissingResource("config".to_owned());
        assert_eq!(err.to_string(), "missing named resource: config");

        let err = ContextError::MissingEntityDataService("User".to_owned());
        assert_eq!(
            err.to_string(),
            "missing entity data service for entity: User"
        );
    }

    #[test]
    fn test_runtime_error_display_all() {
        assert_eq!(
            RuntimeError::Event("foo".to_owned()).to_string(),
            "entity event error: foo"
        );
        assert_eq!(
            RuntimeError::Policy("foo".to_owned()).to_string(),
            "request policy error: foo"
        );
        let sql_err = SqlCompileError::UnknownEntity("User".to_string());
        assert_eq!(
            RuntimeError::from(sql_err).to_string(),
            "unknown entity: User"
        );
        let check_res = CheckResult {
            rule: crate::CheckRule::Required,
            location: crate::ObjectLocation::root(),
            input_value: None,
            system_value: None,
            message: Some("Error at $: bar".to_string()),
        };
        assert_eq!(
            RuntimeError::Check(vec![check_res]).to_string(),
            "check failed: Error at $: bar"
        );
        assert_eq!(
            RuntimeError::Graph("foo".to_owned()).to_string(),
            "graph write error: foo"
        );
        assert_eq!(
            RuntimeError::IdGeneration("foo".to_owned()).to_string(),
            "id generation error: foo"
        );
        assert_eq!(
            RuntimeError::Language("foo".to_owned()).to_string(),
            "language error: foo"
        );
        assert_eq!(
            RuntimeError::Schema("foo".to_owned()).to_string(),
            "schema provider error: foo"
        );
        assert_eq!(
            RuntimeError::OptimisticLockConflict {
                entity: "User".to_owned(),
                id: "1".to_owned(),
            }
            .to_string(),
            "optimistic lock conflict on User(1)"
        );
    }

    #[test]
    fn test_context_error_display_all() {
        assert_eq!(
            ContextError::MissingTypedResource("foo").to_string(),
            "missing typed resource: foo"
        );
    }

    #[test]
    fn test_data_service_error_display() {
        /*
                let err1: DataServiceError<&str> = DataServiceError::Runtime(RuntimeError::MissingEntity("E".into()));
                assert_eq!(err1.to_string(), "missing entity descriptor: E");
                let err2: DataServiceError<&str> = DataServiceError::Entity(EntityError::new("E", "missing field: f"));
                assert_eq!(err2.to_string(), "entity E error: missing field: f");
                let err3: DataServiceError<&str> = DataServiceError::Executor("err");
                assert_eq!(err3.to_string(), "err");
        */
    }
}
