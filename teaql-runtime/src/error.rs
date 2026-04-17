use teaql_core::EntityError;
use teaql_sql::SqlCompileError;

#[derive(Debug)]
pub enum RuntimeError {
    MissingEntity(String),
    SqlCompile(SqlCompileError),
    Behavior(String),
    IdGeneration(String),
    MissingRelation { entity: String, relation: String },
    OptimisticLockConflict { entity: String, id: String },
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingEntity(entity) => write!(f, "missing entity descriptor: {entity}"),
            Self::SqlCompile(err) => err.fmt(f),
            Self::Behavior(message) => write!(f, "repository behavior error: {message}"),
            Self::IdGeneration(message) => write!(f, "id generation error: {message}"),
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
    MissingRepository(String),
}

impl std::fmt::Display for ContextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingResource(name) => write!(f, "missing named resource: {name}"),
            Self::MissingTypedResource(name) => write!(f, "missing typed resource: {name}"),
            Self::MissingRepository(name) => write!(f, "missing repository for entity: {name}"),
        }
    }
}

impl std::error::Error for ContextError {}

#[derive(Debug)]
pub enum RepositoryError<ExecError> {
    Runtime(RuntimeError),
    Entity(EntityError),
    Executor(ExecError),
}

impl<ExecError> std::fmt::Display for RepositoryError<ExecError>
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

impl<ExecError> std::error::Error for RepositoryError<ExecError> where
    ExecError: std::error::Error + 'static
{
}
