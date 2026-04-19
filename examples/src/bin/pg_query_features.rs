use teaql_core::{BinaryOp, Expr, SelectQuery, TeaqlEntity, Value};
use teaql_dialect_pg::PostgresDialect;
use teaql_examples::{Order, PgSyncExecutor, postgres_context, reset_postgres_schema};
use teaql_runtime::sqlx_support::PgMutationExecutor;
use teaql_sql::SqlDialect;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = std::env::var("TEAQL_TEST_PG_URL").ok() else {
        println!("set TEAQL_TEST_PG_URL to run the PostgreSQL query features example");
        return Ok(());
    };

    let pool = sqlx::PgPool::connect(&database_url).await?;
    let executor = PgMutationExecutor::new(pool.clone());
    reset_postgres_schema(&pool, &executor).await?;

    sqlx::query(
        "INSERT INTO example_orders (id, version, name) VALUES
            (1, 1, 'Robert'),
            (2, 2, 'Rupert'),
            (3, 3, 'Alice')",
    )
    .execute(&pool)
    .await?;

    let ctx = postgres_context(executor.clone());
    let repo = ctx.resolve_repository::<PostgresDialect, PgSyncExecutor>("Order")?;

    let feature_query = repo
        .select()
        .project("id")
        .project("name")
        .project_expr("nameSoundex", Expr::soundex(Expr::column("name")))
        .filter(
            Expr::sound_like("name", "Robert")
                .and_expr(Expr::in_large(
                    "id",
                    vec![Value::from(1_u64), Value::from(2_u64), Value::from(3_u64)],
                ))
                .and_expr(Expr::not_in_large("name", vec![Value::from("Alice")])),
        )
        .order_expr_asc(Expr::soundex(Expr::column("name")));
    let rows = repo.fetch_all(&feature_query)?;

    let aggregate_query = SelectQuery::new("Order")
        .count("total")
        .sum("version", "versionSum")
        .stddev("version", "versionStddev")
        .having(Expr::binary(
            Expr::count_all(),
            BinaryOp::Gt,
            Expr::value(1_i64),
        ));
    let aggregate_rows = repo.fetch_all(&aggregate_query)?;

    let compiled = PostgresDialect.compile_select(&Order::entity_descriptor(), &feature_query)?;
    println!("pg feature SQL: {}", compiled.sql);
    println!("pg feature rows: {rows:?}");
    println!("pg aggregate rows: {aggregate_rows:?}");
    Ok(())
}
