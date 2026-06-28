use teaql_core::{SelectQuery, SmartList, TeaqlEntity};
use teaql_macros::TeaqlEntity;
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::{
    AuditedSaveExt, EntityDataServiceBehavior, InMemoryEntityDataServiceBehaviorRegistry,
    InMemoryEntityRegistry, InMemoryMetadataStore, UserContext,
};
use teaql_sql::SqlDataServiceExecutor;

#[derive(Clone, Debug, PartialEq, TeaqlEntity)]
#[teaql(entity = "School", table = "school")]
pub struct School {
    #[teaql(skip)]
    pub root: teaql_runtime::EntityRoot,
    #[teaql(id)]
    pub id: u64,
    pub name: String,
    #[teaql(relation(target = "Student", local_key = "id", foreign_key = "school_id", many))]
    pub students: SmartList<Student>,
}

#[derive(Clone, Debug, PartialEq, TeaqlEntity)]
#[teaql(entity = "Student", table = "student")]
pub struct Student {
    #[teaql(skip)]
    pub root: teaql_runtime::EntityRoot,
    #[teaql(id)]
    pub id: u64,
    #[teaql(column = "school_id")]
    pub school_id: u64,
    pub name: String,
    #[teaql(relation(target = "School", local_key = "school_id", foreign_key = "id"))]
    pub school: Option<School>,
}

pub struct SchoolRelations;

impl EntityDataServiceBehavior for SchoolRelations {
    fn relation_loads(&self, _ctx: &UserContext) -> Vec<String> {
        vec!["students".to_owned()]
    }
}

pub fn metadata() -> InMemoryMetadataStore {
    InMemoryMetadataStore::new()
        .with_entity(School::entity_descriptor())
        .with_entity(Student::entity_descriptor())
}

pub fn entity_registry() -> InMemoryEntityRegistry {
    InMemoryEntityRegistry::new()
        .with_entity("School")
        .with_entity("Student")
}

pub fn behavior_registry() -> InMemoryEntityDataServiceBehaviorRegistry {
    InMemoryEntityDataServiceBehaviorRegistry::new().with_behavior("School", SchoolRelations)
}

pub fn sqlite_context(executor: SqliteMutationExecutor) -> UserContext {
    let mut ctx = UserContext::new()
        .with_metadata(metadata())
        .with_entity_registry(entity_registry())
        .with_entity_data_service_behavior_registry(behavior_registry());
    ctx.use_sqlite_provider(executor.clone());

    // register_executor replaces insert_resource — it also sets up DynGraphSaver
    // so that Audited::save(&ctx) works.
    let data_service = SqlDataServiceExecutor::new(SqliteDialect, executor, metadata());
    ctx.register_executor(data_service);
    ctx
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connection = rusqlite::Connection::open_in_memory()?;
    let executor = SqliteMutationExecutor::from_connection(connection);

    {
        let conn = executor.connection();
        let conn = conn.lock().unwrap();
        conn.execute("DROP TABLE IF EXISTS student", []).unwrap();
        conn.execute("DROP TABLE IF EXISTS school", []).unwrap();
    }
    executor
        .ensure_schema(
            &SqliteDialect,
            &[
                &School::entity_descriptor(),
                &Student::entity_descriptor(),
            ],
        )
        .unwrap();

    let ctx = sqlite_context(executor);

    // ---- NEW API: school.audit_as("...").save(&ctx).await? ----
    use teaql_core::Entity;

    let school = School {
        root: Default::default(),
        id: 1,
        name: "My School".to_owned(),
        students: SmartList::from(vec![
            Student {
                root: Default::default(),
                id: 10,
                school_id: 1,
                name: "Alice".to_owned(),
                school: None,
            },
            Student {
                root: Default::default(),
                id: 11,
                school_id: 1,
                name: "Bob".to_owned(),
                school: None,
            },
        ]),
    };

    school.audit_as("创建学校").save(&ctx).await?;

    // Verify: fetch back with relations
    let data_service = ctx.entity_data_service::<teaql_sql::SqlDataServiceExecutor<
        SqliteDialect,
        SqliteMutationExecutor,
        teaql_runtime::InMemoryMetadataStore,
    >>("School")?;

    let schools = data_service
        .fetch_enhanced_entities::<School>(&SelectQuery::new("School").order_asc("id"))
        .await?;

    println!("school.audit_as(\"创建学校\").save(&ctx) succeeded!");
    println!("Schools: {:#?}", schools);
    Ok(())
}
