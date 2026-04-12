use teaql_core::{
    Aggregate, AggregateFunction, BinaryOp, DeleteCommand, EntityDescriptor, Expr, InsertCommand,
    OrderBy, RecoverCommand, SelectQuery, SortDirection, UpdateCommand, Value,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseKind {
    PostgreSql,
    Sqlite,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompiledQuery {
    pub sql: String,
    pub params: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlCompileError {
    UnknownField(String),
    EmptyInList,
    MissingIdProperty(String),
    MissingVersionProperty(String),
    EmptyMutation(String),
    InvalidRecoverVersion(i64),
}

impl std::fmt::Display for SqlCompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownField(field) => write!(f, "unknown field: {field}"),
            Self::EmptyInList => write!(f, "IN requires at least one value"),
            Self::MissingIdProperty(entity) => write!(f, "entity {entity} has no id property"),
            Self::MissingVersionProperty(entity) => {
                write!(f, "entity {entity} has no version property")
            }
            Self::EmptyMutation(kind) => write!(f, "{kind} requires at least one writable field"),
            Self::InvalidRecoverVersion(version) => {
                write!(f, "recover requires a negative version, got {version}")
            }
        }
    }
}

impl std::error::Error for SqlCompileError {}

pub trait SqlDialect {
    fn kind(&self) -> DatabaseKind;
    fn quote_ident(&self, ident: &str) -> String;
    fn placeholder(&self, index: usize) -> String;

    fn compile_select(
        &self,
        entity: &EntityDescriptor,
        query: &SelectQuery,
    ) -> Result<CompiledQuery, SqlCompileError> {
        let projection = if query.aggregates.is_empty() {
            if query.projection.is_empty() {
                "*".to_owned()
            } else {
                query.projection
                    .iter()
                    .map(|field| self.column_sql(entity, field))
                    .collect::<Result<Vec<_>, _>>()?
                    .join(", ")
            }
        } else {
            self.aggregate_projection(entity, &query.aggregates)?
        };

        let mut params = Vec::new();
        let mut sql = format!(
            "SELECT {projection} FROM {}",
            self.quote_ident(&entity.table_name)
        );

        if let Some(filter) = &query.filter {
            let where_sql = self.compile_expr(entity, filter, &mut params)?;
            sql.push_str(" WHERE ");
            sql.push_str(&where_sql);
        }

        if !query.group_by.is_empty() {
            let group_by = query
                .group_by
                .iter()
                .map(|field| self.column_sql(entity, field))
                .collect::<Result<Vec<_>, _>>()?
                .join(", ");
            sql.push_str(" GROUP BY ");
            sql.push_str(&group_by);
        }

        if !query.order_by.is_empty() {
            let order_by = query
                .order_by
                .iter()
                .map(|order| self.order_by_sql(entity, order))
                .collect::<Result<Vec<_>, _>>()?
                .join(", ");
            sql.push_str(" ORDER BY ");
            sql.push_str(&order_by);
        }

        if let Some(slice) = query.slice {
            if let Some(limit) = slice.limit {
                sql.push_str(&format!(" LIMIT {limit}"));
            }
            if slice.offset > 0 {
                sql.push_str(&format!(" OFFSET {}", slice.offset));
            }
        }

        Ok(CompiledQuery { sql, params })
    }

    fn compile_insert(
        &self,
        entity: &EntityDescriptor,
        command: &InsertCommand,
    ) -> Result<CompiledQuery, SqlCompileError> {
        let mut columns = Vec::new();
        let mut placeholders = Vec::new();
        let mut params = Vec::new();

        for property in &entity.properties {
            if let Some(value) = command.values.get(&property.name) {
                columns.push(self.quote_ident(&property.column_name));
                params.push(value.clone());
                placeholders.push(self.placeholder(params.len()));
            }
        }

        if columns.is_empty() {
            return Err(SqlCompileError::EmptyMutation("insert".to_owned()));
        }

        Ok(CompiledQuery {
            sql: format!(
                "INSERT INTO {} ({}) VALUES ({})",
                self.quote_ident(&entity.table_name),
                columns.join(", "),
                placeholders.join(", ")
            ),
            params,
        })
    }

    fn compile_update(
        &self,
        entity: &EntityDescriptor,
        command: &UpdateCommand,
    ) -> Result<CompiledQuery, SqlCompileError> {
        let id_property = entity
            .id_property()
            .ok_or_else(|| SqlCompileError::MissingIdProperty(entity.name.clone()))?;
        let mut assignments = Vec::new();
        let mut params = Vec::new();

        for property in &entity.properties {
            if property.is_id {
                continue;
            }
            if let Some(value) = command.values.get(&property.name) {
                params.push(value.clone());
                assignments.push(format!(
                    "{} = {}",
                    self.quote_ident(&property.column_name),
                    self.placeholder(params.len())
                ));
            }
        }

        if let Some(expected_version) = command.expected_version {
            let version_property = entity
                .version_property()
                .ok_or_else(|| SqlCompileError::MissingVersionProperty(entity.name.clone()))?;
            params.push(Value::I64(expected_version + 1));
            assignments.push(format!(
                "{} = {}",
                self.quote_ident(&version_property.column_name),
                self.placeholder(params.len())
            ));
        }

        if assignments.is_empty() {
            return Err(SqlCompileError::EmptyMutation("update".to_owned()));
        }

        params.push(command.id.clone());
        let mut predicates = vec![format!(
            "{} = {}",
            self.quote_ident(&id_property.column_name),
            self.placeholder(params.len())
        )];

        if let Some(expected_version) = command.expected_version {
            let version_property = entity
                .version_property()
                .ok_or_else(|| SqlCompileError::MissingVersionProperty(entity.name.clone()))?;
            params.push(Value::I64(expected_version));
            predicates.push(format!(
                "{} = {}",
                self.quote_ident(&version_property.column_name),
                self.placeholder(params.len())
            ));
        }

        Ok(CompiledQuery {
            sql: format!(
                "UPDATE {} SET {} WHERE {}",
                self.quote_ident(&entity.table_name),
                assignments.join(", "),
                predicates.join(" AND ")
            ),
            params,
        })
    }

    fn compile_delete(
        &self,
        entity: &EntityDescriptor,
        command: &DeleteCommand,
    ) -> Result<CompiledQuery, SqlCompileError> {
        let id_property = entity
            .id_property()
            .ok_or_else(|| SqlCompileError::MissingIdProperty(entity.name.clone()))?;
        let mut params = Vec::new();

        if command.soft_delete {
            let version_property = entity
                .version_property()
                .ok_or_else(|| SqlCompileError::MissingVersionProperty(entity.name.clone()))?;
            params.push(match command.expected_version {
                Some(version) => Value::I64(-(version + 1)),
                None => Value::I64(-1),
            });

            params.push(command.id.clone());
            let mut predicates = vec![format!(
                "{} = {}",
                self.quote_ident(&id_property.column_name),
                self.placeholder(params.len())
            )];

            if let Some(expected_version) = command.expected_version {
                params.push(Value::I64(expected_version));
                predicates.push(format!(
                    "{} = {}",
                    self.quote_ident(&version_property.column_name),
                    self.placeholder(params.len())
                ));
            }

            return Ok(CompiledQuery {
                sql: format!(
                    "UPDATE {} SET {} = {} WHERE {}",
                    self.quote_ident(&entity.table_name),
                    self.quote_ident(&version_property.column_name),
                    self.placeholder(1),
                    predicates.join(" AND ")
                ),
                params,
            });
        }

        params.push(command.id.clone());
        let mut predicates = vec![format!(
            "{} = {}",
            self.quote_ident(&id_property.column_name),
            self.placeholder(params.len())
        )];

        if let Some(expected_version) = command.expected_version {
            let version_property = entity
                .version_property()
                .ok_or_else(|| SqlCompileError::MissingVersionProperty(entity.name.clone()))?;
            params.push(Value::I64(expected_version));
            predicates.push(format!(
                "{} = {}",
                self.quote_ident(&version_property.column_name),
                self.placeholder(params.len())
            ));
        }

        Ok(CompiledQuery {
            sql: format!(
                "DELETE FROM {} WHERE {}",
                self.quote_ident(&entity.table_name),
                predicates.join(" AND ")
            ),
            params,
        })
    }

    fn compile_recover(
        &self,
        entity: &EntityDescriptor,
        command: &RecoverCommand,
    ) -> Result<CompiledQuery, SqlCompileError> {
        if command.expected_version >= 0 {
            return Err(SqlCompileError::InvalidRecoverVersion(
                command.expected_version,
            ));
        }

        let id_property = entity
            .id_property()
            .ok_or_else(|| SqlCompileError::MissingIdProperty(entity.name.clone()))?;
        let version_property = entity
            .version_property()
            .ok_or_else(|| SqlCompileError::MissingVersionProperty(entity.name.clone()))?;
        let params = vec![
            Value::I64(-command.expected_version + 1),
            command.id.clone(),
            Value::I64(command.expected_version),
        ];

        Ok(CompiledQuery {
            sql: format!(
                "UPDATE {} SET {} = {} WHERE {} = {} AND {} = {}",
                self.quote_ident(&entity.table_name),
                self.quote_ident(&version_property.column_name),
                self.placeholder(1),
                self.quote_ident(&id_property.column_name),
                self.placeholder(2),
                self.quote_ident(&version_property.column_name),
                self.placeholder(3),
            ),
            params,
        })
    }

    fn column_sql(
        &self,
        entity: &EntityDescriptor,
        field: &str,
    ) -> Result<String, SqlCompileError> {
        let property = entity
            .property_by_name(field)
            .ok_or_else(|| SqlCompileError::UnknownField(field.to_owned()))?;
        Ok(self.quote_ident(&property.column_name))
    }

    fn order_by_sql(
        &self,
        entity: &EntityDescriptor,
        order_by: &OrderBy,
    ) -> Result<String, SqlCompileError> {
        let field = self.column_sql(entity, &order_by.field)?;
        let direction = match order_by.direction {
            SortDirection::Asc => "ASC",
            SortDirection::Desc => "DESC",
        };
        Ok(format!("{field} {direction}"))
    }

    fn aggregate_projection(
        &self,
        entity: &EntityDescriptor,
        aggregates: &[Aggregate],
    ) -> Result<String, SqlCompileError> {
        aggregates
            .iter()
            .map(|aggregate| {
                let function = match aggregate.function {
                    AggregateFunction::Count => "COUNT",
                    AggregateFunction::Sum => "SUM",
                    AggregateFunction::Avg => "AVG",
                    AggregateFunction::Min => "MIN",
                    AggregateFunction::Max => "MAX",
                };
                let field = self.column_sql(entity, &aggregate.field)?;
                Ok(format!(
                    "{function}({field}) AS {}",
                    self.quote_ident(&aggregate.alias)
                ))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|parts| parts.join(", "))
    }

    fn compile_expr(
        &self,
        entity: &EntityDescriptor,
        expr: &Expr,
        params: &mut Vec<Value>,
    ) -> Result<String, SqlCompileError> {
        match expr {
            Expr::Column(name) => self.column_sql(entity, name),
            Expr::Value(value) => {
                params.push(value.clone());
                Ok(self.placeholder(params.len()))
            }
            Expr::Binary { left, op, right } => {
                if *op == BinaryOp::In {
                    return self.compile_in(entity, left, right, params);
                }
                let lhs = self.compile_expr(entity, left, params)?;
                let rhs = self.compile_expr(entity, right, params)?;
                let op = match op {
                    BinaryOp::Eq => "=",
                    BinaryOp::Ne => "!=",
                    BinaryOp::Gt => ">",
                    BinaryOp::Gte => ">=",
                    BinaryOp::Lt => "<",
                    BinaryOp::Lte => "<=",
                    BinaryOp::Like => "LIKE",
                    BinaryOp::In => unreachable!(),
                };
                Ok(format!("({lhs} {op} {rhs})"))
            }
            Expr::Between { expr, lower, upper } => {
                let expr = self.compile_expr(entity, expr, params)?;
                let lower = self.compile_expr(entity, lower, params)?;
                let upper = self.compile_expr(entity, upper, params)?;
                Ok(format!("({expr} BETWEEN {lower} AND {upper})"))
            }
            Expr::IsNull(expr) => {
                let expr = self.compile_expr(entity, expr, params)?;
                Ok(format!("({expr} IS NULL)"))
            }
            Expr::IsNotNull(expr) => {
                let expr = self.compile_expr(entity, expr, params)?;
                Ok(format!("({expr} IS NOT NULL)"))
            }
            Expr::And(parts) => self.compile_joined(entity, parts, "AND", params),
            Expr::Or(parts) => self.compile_joined(entity, parts, "OR", params),
            Expr::Not(expr) => {
                let expr = self.compile_expr(entity, expr, params)?;
                Ok(format!("(NOT {expr})"))
            }
        }
    }

    fn compile_joined(
        &self,
        entity: &EntityDescriptor,
        parts: &[Expr],
        joiner: &str,
        params: &mut Vec<Value>,
    ) -> Result<String, SqlCompileError> {
        let compiled = parts
            .iter()
            .map(|part| self.compile_expr(entity, part, params))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(format!("({})", compiled.join(&format!(" {joiner} "))))
    }

    fn compile_in(
        &self,
        entity: &EntityDescriptor,
        left: &Expr,
        right: &Expr,
        params: &mut Vec<Value>,
    ) -> Result<String, SqlCompileError> {
        let lhs = self.compile_expr(entity, left, params)?;
        match right {
            Expr::Value(Value::List(values)) => {
                if values.is_empty() {
                    return Err(SqlCompileError::EmptyInList);
                }
                let mut placeholders = Vec::with_capacity(values.len());
                for value in values {
                    params.push(value.clone());
                    placeholders.push(self.placeholder(params.len()));
                }
                Ok(format!("({lhs} IN ({}))", placeholders.join(", ")))
            }
            _ => {
                let rhs = self.compile_expr(entity, right, params)?;
                Ok(format!("({lhs} IN ({rhs}))"))
            }
        }
    }
}
