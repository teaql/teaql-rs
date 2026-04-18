use teaql_core::{
    AggregateFunction, BinaryOp, DataType, DeleteCommand, EntityDescriptor, Expr, ExprFunction,
    InsertCommand, OrderBy, PropertyDescriptor, RecoverCommand, SelectQuery, SortDirection,
    UpdateCommand, Value,
};

use crate::{CompiledQuery, DatabaseKind, SqlCompileError};

pub trait SqlDialect {
    fn kind(&self) -> DatabaseKind;
    fn quote_ident(&self, ident: &str) -> String;
    fn placeholder(&self, index: usize) -> String;

    fn schema_setup_sqls(&self) -> &'static [&'static str] {
        &[]
    }

    fn schema_type_sql(
        &self,
        data_type: DataType,
        _property: &PropertyDescriptor,
    ) -> Result<&'static str, SqlCompileError> {
        match data_type {
            DataType::Bool => Ok("BOOLEAN"),
            DataType::I64 | DataType::U64 => Ok("INTEGER"),
            DataType::F64 => Ok("REAL"),
            DataType::Text | DataType::Json | DataType::Date | DataType::Timestamp => Ok("TEXT"),
        }
    }

    fn column_definition_sql(
        &self,
        property: &PropertyDescriptor,
    ) -> Result<String, SqlCompileError> {
        let mut parts = vec![
            self.quote_ident(&property.column_name),
            self.schema_type_sql(property.data_type, property)?
                .to_owned(),
        ];

        if property.is_id {
            parts.push("PRIMARY KEY".to_owned());
        }
        if property.is_id || !property.nullable {
            parts.push("NOT NULL".to_owned());
        }

        Ok(parts.join(" "))
    }

    fn compile_create_table(&self, entity: &EntityDescriptor) -> Result<String, SqlCompileError> {
        let columns = entity
            .properties
            .iter()
            .map(|property| self.column_definition_sql(property))
            .collect::<Result<Vec<_>, _>>()?
            .join(", ");
        Ok(format!(
            "CREATE TABLE IF NOT EXISTS {} ({columns})",
            self.quote_ident(&entity.table_name)
        ))
    }

    fn compile_add_column(
        &self,
        entity: &EntityDescriptor,
        property: &PropertyDescriptor,
    ) -> Result<String, SqlCompileError> {
        Ok(format!(
            "ALTER TABLE {} ADD COLUMN {}",
            self.quote_ident(&entity.table_name),
            self.column_definition_sql(property)?
        ))
    }

    fn compile_select(
        &self,
        entity: &EntityDescriptor,
        query: &SelectQuery,
    ) -> Result<CompiledQuery, SqlCompileError> {
        let mut params = Vec::new();
        let sql = self.compile_select_sql(entity, query, &mut params)?;
        Ok(CompiledQuery { sql, params })
    }

    fn compile_select_sql(
        &self,
        entity: &EntityDescriptor,
        query: &SelectQuery,
        params: &mut Vec<Value>,
    ) -> Result<String, SqlCompileError> {
        let projection = if query.aggregates.is_empty() {
            self.select_projection(entity, query, params)?
        } else {
            self.aggregate_projection(entity, query, params)?
        };

        let mut sql = format!(
            "SELECT {projection} FROM {}",
            self.quote_ident(&entity.table_name)
        );

        if let Some(filter) = &query.filter {
            let where_sql = self.compile_expr(entity, filter, params)?;
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

        if let Some(having) = &query.having {
            let having_sql = self.compile_expr(entity, having, params)?;
            sql.push_str(" HAVING ");
            sql.push_str(&having_sql);
        }

        if !query.order_by.is_empty() {
            let order_by = query
                .order_by
                .iter()
                .map(|order| self.order_by_sql(entity, order, params))
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

        Ok(sql)
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
        params: &mut Vec<Value>,
    ) -> Result<String, SqlCompileError> {
        let field = if let Some(expr) = &order_by.expr {
            self.compile_expr(entity, expr, params)?
        } else {
            self.column_sql(entity, &order_by.field)?
        };
        let direction = match order_by.direction {
            SortDirection::Asc => "ASC",
            SortDirection::Desc => "DESC",
        };
        Ok(format!("{field} {direction}"))
    }

    fn select_projection(
        &self,
        entity: &EntityDescriptor,
        query: &SelectQuery,
        params: &mut Vec<Value>,
    ) -> Result<String, SqlCompileError> {
        if query.projection.is_empty() && query.expr_projection.is_empty() {
            return Ok("*".to_owned());
        }
        let mut parts = query
            .projection
            .iter()
            .map(|field| self.column_sql(entity, field))
            .collect::<Result<Vec<_>, _>>()?;
        for projection in &query.expr_projection {
            let expr = self.compile_expr(entity, &projection.expr, params)?;
            parts.push(format!("{expr} AS {}", self.quote_ident(&projection.alias)));
        }
        Ok(parts.join(", "))
    }

    fn aggregate_projection(
        &self,
        entity: &EntityDescriptor,
        query: &SelectQuery,
        params: &mut Vec<Value>,
    ) -> Result<String, SqlCompileError> {
        let mut parts = Vec::new();
        for field in query.group_by.iter().chain(query.projection.iter()) {
            let column = self.column_sql(entity, field)?;
            if !parts.contains(&column) {
                parts.push(column);
            }
        }
        for projection in &query.expr_projection {
            let expr = self.compile_expr(entity, &projection.expr, params)?;
            let aliased = format!("{expr} AS {}", self.quote_ident(&projection.alias));
            if !parts.contains(&aliased) {
                parts.push(aliased);
            }
        }
        parts.extend(
            query
                .aggregates
                .iter()
                .map(|aggregate| {
                    let field = if aggregate.function == AggregateFunction::Count
                        && aggregate.field == "*"
                    {
                        "*".to_owned()
                    } else {
                        self.column_sql(entity, &aggregate.field)?
                    };
                    let call = self.aggregate_call_sql(aggregate.function, &field);
                    Ok(format!("{call} AS {}", self.quote_ident(&aggregate.alias)))
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
        Ok(parts.join(", "))
    }

    fn aggregate_call_sql(&self, function: AggregateFunction, field: &str) -> String {
        let function_sql = self.aggregate_function_sql(function);
        format!("{function_sql}({field})")
    }

    fn aggregate_function_sql(&self, function: AggregateFunction) -> &'static str {
        match function {
            AggregateFunction::Count => "COUNT",
            AggregateFunction::Sum => "SUM",
            AggregateFunction::Avg => "AVG",
            AggregateFunction::Min => "MIN",
            AggregateFunction::Max => "MAX",
            AggregateFunction::Stddev => "STDDEV",
            AggregateFunction::StddevPop => "STDDEV_POP",
            AggregateFunction::VarSamp => "VAR_SAMP",
            AggregateFunction::VarPop => "VAR_POP",
            AggregateFunction::BitAnd => "BIT_AND",
            AggregateFunction::BitOr => "BIT_OR",
            AggregateFunction::BitXor => "BIT_XOR",
        }
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
            Expr::Function { function, args } => {
                self.compile_function(entity, *function, args, params)
            }
            Expr::Binary { left, op, right } => {
                if matches!(
                    op,
                    BinaryOp::In | BinaryOp::NotIn | BinaryOp::InLarge | BinaryOp::NotInLarge
                ) {
                    return self.compile_in(entity, left, *op, right, params);
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
                    BinaryOp::NotLike => "NOT LIKE",
                    BinaryOp::In | BinaryOp::NotIn | BinaryOp::InLarge | BinaryOp::NotInLarge => {
                        unreachable!()
                    }
                };
                Ok(format!("({lhs} {op} {rhs})"))
            }
            Expr::SubQuery {
                left,
                op,
                entity: sub_entity,
                query,
            } => self.compile_subquery(entity, left, *op, sub_entity, query, params),
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

    fn compile_function(
        &self,
        entity: &EntityDescriptor,
        function: ExprFunction,
        args: &[Expr],
        params: &mut Vec<Value>,
    ) -> Result<String, SqlCompileError> {
        match function {
            ExprFunction::Soundex => {
                let [arg] = args else {
                    return Err(SqlCompileError::InvalidFunctionArguments(
                        "SOUNDEX expects exactly one argument".to_owned(),
                    ));
                };
                let arg = self.compile_expr(entity, arg, params)?;
                Ok(format!("SOUNDEX({arg})"))
            }
            ExprFunction::Gbk => {
                let [arg] = args else {
                    return Err(SqlCompileError::InvalidFunctionArguments(
                        "GBK expects exactly one argument".to_owned(),
                    ));
                };
                let arg = self.compile_expr(entity, arg, params)?;
                Ok(format!("convert_to({arg}, 'GBK')"))
            }
            ExprFunction::Count if args.is_empty() => Ok("COUNT(*)".to_owned()),
            ExprFunction::Count => self.compile_single_arg_function(entity, "COUNT", args, params),
            ExprFunction::Sum => self.compile_single_arg_function(entity, "SUM", args, params),
            ExprFunction::Avg => self.compile_single_arg_function(entity, "AVG", args, params),
            ExprFunction::Min => self.compile_single_arg_function(entity, "MIN", args, params),
            ExprFunction::Max => self.compile_single_arg_function(entity, "MAX", args, params),
            ExprFunction::Stddev => {
                self.compile_single_arg_function(entity, "STDDEV", args, params)
            }
            ExprFunction::StddevPop => {
                self.compile_single_arg_function(entity, "STDDEV_POP", args, params)
            }
            ExprFunction::VarSamp => {
                self.compile_single_arg_function(entity, "VAR_SAMP", args, params)
            }
            ExprFunction::VarPop => {
                self.compile_single_arg_function(entity, "VAR_POP", args, params)
            }
            ExprFunction::BitAnd => {
                self.compile_single_arg_function(entity, "BIT_AND", args, params)
            }
            ExprFunction::BitOr => self.compile_single_arg_function(entity, "BIT_OR", args, params),
            ExprFunction::BitXor => {
                self.compile_single_arg_function(entity, "BIT_XOR", args, params)
            }
        }
    }

    fn compile_single_arg_function(
        &self,
        entity: &EntityDescriptor,
        function: &str,
        args: &[Expr],
        params: &mut Vec<Value>,
    ) -> Result<String, SqlCompileError> {
        let [arg] = args else {
            return Err(SqlCompileError::InvalidFunctionArguments(format!(
                "{function} expects exactly one argument"
            )));
        };
        let arg = self.compile_expr(entity, arg, params)?;
        Ok(format!("{function}({arg})"))
    }

    fn compile_subquery(
        &self,
        entity: &EntityDescriptor,
        left: &Expr,
        op: BinaryOp,
        sub_entity: &EntityDescriptor,
        query: &SelectQuery,
        params: &mut Vec<Value>,
    ) -> Result<String, SqlCompileError> {
        let lhs = self.compile_expr(entity, left, params)?;
        let operator = match op {
            BinaryOp::In | BinaryOp::InLarge => "IN",
            BinaryOp::NotIn | BinaryOp::NotInLarge => "NOT IN",
            _ => return Err(SqlCompileError::InvalidSubQueryOperator(format!("{op:?}"))),
        };
        let subquery = self.compile_select_sql(sub_entity, query, params)?;
        Ok(format!("({lhs} {operator} ({subquery}))"))
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
        op: BinaryOp,
        right: &Expr,
        params: &mut Vec<Value>,
    ) -> Result<String, SqlCompileError> {
        let lhs = self.compile_expr(entity, left, params)?;
        let operator = match op {
            BinaryOp::In | BinaryOp::InLarge => "IN",
            BinaryOp::NotIn | BinaryOp::NotInLarge => "NOT IN",
            _ => unreachable!(),
        };
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
                Ok(format!("({lhs} {operator} ({}))", placeholders.join(", ")))
            }
            _ => {
                let rhs = self.compile_expr(entity, right, params)?;
                Ok(format!("({lhs} {operator} ({rhs}))"))
            }
        }
    }
}
