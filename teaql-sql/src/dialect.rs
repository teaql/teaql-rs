use teaql_core::{
    Aggregate, AggregateFunction, BinaryOp, DataType, DeleteCommand, EntityDescriptor, Expr,
    ExprFunction, OrderBy, PropertyDescriptor, RecoverCommand, SelectQuery, SortDirection, Value,
};

use crate::{CompiledQuery, DatabaseKind, SqlCompileError};

const SQL_KEYWORDS: &[&str] = &[
    "all", "alter", "and", "as", "asc", "between", "by", "case", "create", "delete", "desc",
    "distinct", "drop", "exists", "false", "from", "group", "having", "in", "insert", "into", "is",
    "join", "like", "limit", "not", "null", "offset", "on", "or", "order", "select", "set",
    "table", "true", "type", "union", "update", "values", "where",
];

pub fn quote_identifier_if_needed(ident: &str, quote: char) -> String {
    if is_wrapped_identifier(ident) {
        return ident.to_owned();
    }
    if needs_quoted_identifier(ident) {
        let quote_string = quote.to_string();
        let escaped = ident.replace(quote, &(quote_string.clone() + &quote_string));
        return format!("{quote}{escaped}{quote}");
    }
    ident.to_owned()
}

/// Keep generated SQL identifiers within a provider's byte limit without
/// relying on its silent truncation rules. Short names remain unchanged; long
/// names retain a readable prefix plus a stable FNV-1a suffix.
pub fn bounded_sql_identifier(full: &str, max_bytes: usize) -> String {
    if full.len() <= max_bytes {
        return full.to_owned();
    }
    let hash = full.bytes().fold(0xcbf29ce484222325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    });
    let suffix = format!("_{hash:016X}");
    assert!(
        max_bytes >= suffix.len(),
        "SQL identifier byte limit is too small"
    );
    let mut end = max_bytes - suffix.len();
    while !full.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{}", &full[..end], suffix)
}

/// The ordinary indexes derived from model properties. Providers may cap the
/// generated name to their physical identifier limit, but both DDL and schema
/// verification must consume this same list of specs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaIndexSpec {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

pub fn schema_index_specs(
    entity: &EntityDescriptor,
    max_name_bytes: Option<usize>,
) -> Vec<SchemaIndexSpec> {
    let name = |full: String| match max_name_bytes {
        Some(limit) => bounded_sql_identifier(&full, limit),
        None => full,
    };
    let mut specs = Vec::new();
    let table_upper = entity.table_name.to_uppercase();
    if let Some(version) = entity
        .properties
        .iter()
        .find(|property| property.is_version)
    {
        let id = entity
            .properties
            .iter()
            .find(|property| property.is_id)
            .map(|property| property.column_name.as_str())
            .unwrap_or("id");
        specs.push(SchemaIndexSpec {
            name: name(format!("PK_{table_upper}_ID_VERSION")),
            table: entity.table_name.clone(),
            columns: vec![id.to_owned(), version.column_name.clone()],
            unique: true,
        });
    }
    for property in &entity.properties {
        if property.name.ends_with("Id")
            || property.name.ends_with("Time")
            || property.name.ends_with("_time")
            || property.name == "create_time"
            || property.name == "update_time"
        {
            specs.push(SchemaIndexSpec {
                name: name(format!(
                    "IDX_{table_upper}_{}",
                    property.column_name.to_uppercase()
                )),
                table: entity.table_name.clone(),
                columns: vec![property.column_name.clone()],
                unique: false,
            });
        }
    }
    specs
}

/// A stable physical name for a model-derived foreign-key constraint.
/// Database providers must use the same name for creation and collision checks.
pub fn schema_foreign_key_name(
    source_table: &str,
    source_column: &str,
    referenced_table: &str,
    referenced_column: &str,
    max_name_bytes: usize,
) -> String {
    let full = format!("FK_{source_table}_{source_column}_{referenced_table}_{referenced_column}")
        .to_uppercase();
    bounded_sql_identifier(&full, max_name_bytes)
}

/// Reject model-owned identifiers that a database would truncate or reject.
/// Generated index names are bounded separately; table and column names must
/// not be rewritten because they are part of the model's storage contract.
pub fn validate_schema_identifier_lengths(
    entities: &[&EntityDescriptor],
    max_bytes: usize,
) -> Result<(), SqlCompileError> {
    let validate = |object: String, identifier: &str| {
        let physical = if let Some(inner) = identifier
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
        {
            inner.replace("\"\"", "\"")
        } else if let Some(inner) = identifier
            .strip_prefix('`')
            .and_then(|value| value.strip_suffix('`'))
        {
            inner.replace("``", "`")
        } else {
            identifier.to_owned()
        };
        let actual_bytes = physical.len();
        if actual_bytes > max_bytes {
            return Err(SqlCompileError::SchemaIdentifierTooLong {
                object,
                identifier: identifier.to_owned(),
                actual_bytes,
                max_bytes,
            });
        }
        Ok(())
    };
    for entity in entities {
        validate(
            format!("table for entity {}", entity.name),
            &entity.table_name,
        )?;
        for property in &entity.properties {
            validate(
                format!(
                    "column for entity {} property {}",
                    entity.name, property.name
                ),
                &property.column_name,
            )?;
        }
    }
    Ok(())
}

/// Returns whether an existing storage length can represent every value allowed
/// by the model. `None` denotes an unbounded storage type such as PostgreSQL
/// `text`.
pub fn storage_length_covers(expected: u32, actual: Option<u32>) -> bool {
    actual.is_none_or(|actual| actual >= expected)
}

/// Returns whether an existing decimal shape can represent every value allowed
/// by the model. Decimal containment requires at least as many fractional and
/// integer digits. An unbounded storage `NUMERIC` covers every bounded model.
pub fn storage_numeric_covers(
    expected_precision: u32,
    expected_scale: u32,
    actual_precision: Option<u32>,
    actual_scale: Option<u32>,
) -> bool {
    match (actual_precision, actual_scale) {
        (None, None) => true,
        (Some(actual_precision), Some(actual_scale)) => {
            actual_scale >= expected_scale
                && actual_precision.saturating_sub(actual_scale)
                    >= expected_precision.saturating_sub(expected_scale)
        }
        _ => false,
    }
}

fn is_wrapped_identifier(ident: &str) -> bool {
    (ident.starts_with('"') && ident.ends_with('"'))
        || (ident.starts_with('`') && ident.ends_with('`'))
        || (ident.starts_with('[') && ident.ends_with(']'))
}

fn needs_quoted_identifier(ident: &str) -> bool {
    if ident.is_empty()
        || SQL_KEYWORDS
            .binary_search(&ident.to_ascii_lowercase().as_str())
            .is_ok()
    {
        return true;
    }
    let mut chars = ident.chars();
    match chars.next() {
        Some(first) if first == '_' || first.is_ascii_alphabetic() => {}
        _ => return true,
    }
    chars.any(|ch| ch != '_' && !ch.is_ascii_alphanumeric())
}

pub trait SqlDialect {
    fn kind(&self) -> DatabaseKind;
    fn quote_ident(&self, ident: &str) -> String;
    fn placeholder(&self, index: usize) -> String;

    /// Whether `IN_LARGE` / `NOT_IN_LARGE` bind the complete value list as a
    /// single dialect-native array parameter instead of scalar placeholders.
    fn large_in_uses_array_param(&self) -> bool {
        false
    }

    fn prefers_small_parent_relation_probes(&self) -> bool {
        false
    }

    fn schema_setup_sqls(&self) -> &'static [&'static str] {
        &[]
    }

    fn schema_type_sql(
        &self,
        data_type: DataType,
        property: &PropertyDescriptor,
    ) -> Result<String, SqlCompileError> {
        match data_type {
            DataType::Bool => Ok("BOOLEAN".to_owned()),
            DataType::I64 | DataType::U64 => Ok("INTEGER".to_owned()),
            DataType::F64 => Ok("REAL".to_owned()),
            DataType::Decimal => match (property.numeric_precision, property.numeric_scale) {
                (Some(precision), Some(scale)) => Ok(format!("NUMERIC({precision},{scale})")),
                _ => Ok("NUMERIC".to_owned()),
            },
            DataType::Text => Ok(format!("VARCHAR({})", property.max_length.unwrap_or(255))),
            DataType::LargeText | DataType::Json | DataType::Date | DataType::Timestamp => {
                Ok("TEXT".to_owned())
            }
        }
    }

    fn column_definition_sql(
        &self,
        property: &PropertyDescriptor,
    ) -> Result<String, SqlCompileError> {
        validate_schema_shape(property)?;
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

    fn schema_indexes_sqls(
        &self,
        entity: &EntityDescriptor,
    ) -> Result<Vec<String>, SqlCompileError> {
        Ok(schema_index_specs(entity, None)
            .into_iter()
            .map(|spec| {
                let columns = spec
                    .columns
                    .iter()
                    .map(|column| self.quote_ident(column))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "{} {} ON {} ({columns})",
                    if spec.unique {
                        "CREATE UNIQUE INDEX IF NOT EXISTS"
                    } else {
                        "CREATE INDEX IF NOT EXISTS"
                    },
                    self.quote_ident(&spec.name),
                    self.quote_ident(&spec.table),
                )
            })
            .collect())
    }

    fn fallback_default_value_sql(&self, data_type: DataType) -> &'static str {
        match data_type {
            DataType::Bool => "FALSE",
            DataType::I64 | DataType::U64 | DataType::F64 | DataType::Decimal => "0",
            DataType::Text | DataType::LargeText => "''",
            DataType::Json => "'{}'",
            DataType::Date => "'1970-01-01'",
            DataType::Timestamp => "'1970-01-01 00:00:00Z'",
        }
    }

    fn compile_add_column(
        &self,
        entity: &EntityDescriptor,
        property: &PropertyDescriptor,
    ) -> Result<String, SqlCompileError> {
        let mut def = self.column_definition_sql(property)?;
        if !property.nullable && !property.is_id {
            def.push_str(" DEFAULT ");
            def.push_str(self.fallback_default_value_sql(property.data_type));
        }
        Ok(format!(
            "ALTER TABLE {} ADD COLUMN {}",
            self.quote_ident(&entity.table_name),
            def
        ))
    }

    fn compile_select(
        &self,
        entity: &EntityDescriptor,
        query: &SelectQuery,
    ) -> Result<CompiledQuery, SqlCompileError> {
        let mut params = Vec::new();
        let sql = self.compile_select_sql(entity, query, &mut params)?;
        Ok(CompiledQuery {
            sql,
            params,
            comment: query.comment.clone(),
        })
    }

    fn compile_select_sql(
        &self,
        entity: &EntityDescriptor,
        query: &SelectQuery,
        params: &mut Vec<Value>,
    ) -> Result<String, SqlCompileError> {
        if let Some(raw_sql) = &query.raw_sql {
            return Ok(raw_sql.clone());
        }

        let mut projection = self.compile_projection(entity, query, params)?;
        let partitioned_slice = query.partition_by.as_deref().zip(query.slice);
        if let Some((partition_by, _)) = partitioned_slice {
            let partition_column = self.column_sql(entity, partition_by)?;
            let window_order = if query.order_by.is_empty() {
                String::new()
            } else {
                let order_by = query
                    .order_by
                    .iter()
                    .map(|order| self.order_by_sql(entity, order, params))
                    .collect::<Result<Vec<_>, _>>()?
                    .join(", ");
                format!(" ORDER BY {order_by}")
            };
            let rank = self.quote_ident(teaql_core::PARTITION_RANK_PROPERTY);
            projection.push_str(&format!(
                ", ROW_NUMBER() OVER (PARTITION BY {partition_column}{window_order}) AS {rank}"
            ));
        }

        let mut sql = format!(
            "SELECT {projection} FROM {}",
            self.quote_ident(&entity.table_name)
        );

        let mut where_parts = Vec::new();
        if let Some(filter) = &query.filter {
            where_parts.push(self.compile_expr(entity, filter, params)?);
        }

        if let Some(search_text) = &query.search_with_text {
            let mut or_parts = Vec::new();
            let like_value = format!("%{}%", search_text);
            for property in &entity.properties {
                if property.data_type == teaql_core::DataType::Text
                    || property.data_type == teaql_core::DataType::LargeText
                {
                    params.push(teaql_core::Value::from(like_value.clone()));
                    or_parts.push(format!(
                        "{} LIKE {}",
                        self.quote_ident(&property.column_name),
                        self.placeholder(params.len())
                    ));
                }
            }
            if !or_parts.is_empty() {
                where_parts.push(format!("({})", or_parts.join(" OR ")));
            }
        }

        where_parts.extend(query.raw_sql_search_criteria.iter().cloned());
        if !where_parts.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_parts.join(" AND "));
        }

        if let Some((_, slice)) = partitioned_slice {
            let rank = self.quote_ident(teaql_core::PARTITION_RANK_PROPERTY);
            let alias = self.quote_ident("__teaql_partitioned");
            let mut predicates = vec![format!("{rank} > {}", slice.offset)];
            if let Some(limit) = slice.limit {
                predicates.push(format!("{rank} <= {}", slice.offset.saturating_add(limit)));
            }
            return Ok(format!(
                "SELECT * FROM ({sql}) AS {alias} WHERE {} ORDER BY {rank}",
                predicates.join(" AND ")
            ));
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
        command: &teaql_core::InsertCommand,
    ) -> Result<CompiledQuery, SqlCompileError> {
        let mut columns = Vec::new();
        let mut placeholders = Vec::new();
        let mut params = Vec::new();

        for property in &entity.properties {
            if let Some(value) = command.values.get(&property.name) {
                columns.push(self.quote_ident(&property.column_name));
                let mut v = value.clone();
                if let Value::Null = v {
                    v = Value::TypedNull(property.data_type);
                }
                params.push(v);
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
            comment: None,
        })
    }

    fn compile_batch_insert(
        &self,
        entity: &EntityDescriptor,
        command: &teaql_core::BatchInsertCommand,
    ) -> Result<CompiledQuery, SqlCompileError> {
        if command.batch_values.is_empty() {
            return Err(SqlCompileError::EmptyMutation("batch_insert".to_owned()));
        }

        let mut columns = Vec::new();
        let first_record = &command.batch_values[0];

        for property in &entity.properties {
            if first_record.contains_key(&property.name) {
                columns.push(property.clone());
            }
        }

        if columns.is_empty() {
            return Err(SqlCompileError::EmptyMutation("batch_insert".to_owned()));
        }

        let column_names: Vec<String> = columns
            .iter()
            .map(|p| self.quote_ident(&p.column_name))
            .collect();
        let mut params = Vec::new();
        let mut values_clauses = Vec::new();

        for record in &command.batch_values {
            let mut row_placeholders = Vec::new();
            for property in &columns {
                let mut value = record
                    .get(&property.name)
                    .cloned()
                    .unwrap_or(teaql_core::Value::Null);
                if let Value::Null = value {
                    value = Value::TypedNull(property.data_type);
                }
                params.push(value);
                row_placeholders.push(self.placeholder(params.len()));
            }
            values_clauses.push(format!("({})", row_placeholders.join(", ")));
        }

        Ok(CompiledQuery {
            sql: format!(
                "INSERT INTO {} ({}) VALUES {}",
                self.quote_ident(&entity.table_name),
                column_names.join(", "),
                values_clauses.join(", ")
            ),
            params,
            comment: None,
        })
    }

    fn compile_update(
        &self,
        entity: &EntityDescriptor,
        command: &teaql_core::UpdateCommand,
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
            if property.is_version && command.expected_version.is_some() {
                continue;
            }
            if let Some(value) = command.values.get(&property.name) {
                let mut v = value.clone();
                if let Value::Null = v {
                    v = Value::TypedNull(property.data_type);
                }
                params.push(v);
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
            comment: None,
        })
    }

    fn compile_batch_update(
        &self,
        entity: &EntityDescriptor,
        command: &teaql_core::BatchUpdateCommand,
    ) -> Result<CompiledQuery, SqlCompileError> {
        if command.batch_values.is_empty() {
            return Err(SqlCompileError::EmptyMutation("batch_update".to_owned()));
        }

        let id_property = entity
            .id_property()
            .ok_or_else(|| SqlCompileError::MissingIdProperty(entity.name.clone()))?;

        let mut params = Vec::new();
        let mut set_clauses = Vec::new();

        // Build CASE statement for each updated field
        for field_name in &command.update_fields {
            let property = entity
                .property_by_name(field_name)
                .ok_or_else(|| SqlCompileError::UnknownField(field_name.clone()))?;

            let mut case_parts = Vec::new();
            case_parts.push(format!(
                "CASE {}",
                self.quote_ident(&id_property.column_name)
            ));

            for (i, record) in command.batch_values.iter().enumerate() {
                let id = &command.batch_ids[i];
                let mut val = record
                    .get(field_name)
                    .cloned()
                    .unwrap_or(teaql_core::Value::Null);
                if let Value::Null = val {
                    val = Value::TypedNull(property.data_type);
                }

                params.push(id.clone());
                let id_ph = self.placeholder(params.len());

                params.push(val);
                let val_ph = self.placeholder(params.len());

                case_parts.push(format!("WHEN {} THEN {}", id_ph, val_ph));
            }

            case_parts.push(format!(
                "ELSE {} END",
                self.quote_ident(&property.column_name)
            ));
            set_clauses.push(format!(
                "{} = {}",
                self.quote_ident(&property.column_name),
                case_parts.join(" ")
            ));
        }

        let mut has_versions = false;
        if let Some(version_property) = entity.version_property() {
            let mut case_parts = Vec::new();
            case_parts.push(format!(
                "CASE {}",
                self.quote_ident(&id_property.column_name)
            ));

            for (i, exp_ver_opt) in command.batch_expected_versions.iter().enumerate() {
                if let Some(exp_ver) = exp_ver_opt {
                    has_versions = true;
                    let id = &command.batch_ids[i];

                    params.push(id.clone());
                    let id_ph = self.placeholder(params.len());

                    params.push(teaql_core::Value::I64(*exp_ver + 1));
                    let val_ph = self.placeholder(params.len());

                    case_parts.push(format!("WHEN {} THEN {}", id_ph, val_ph));
                }
            }

            if has_versions {
                case_parts.push(format!(
                    "ELSE {} END",
                    self.quote_ident(&version_property.column_name)
                ));
                set_clauses.push(format!(
                    "{} = {}",
                    self.quote_ident(&version_property.column_name),
                    case_parts.join(" ")
                ));
            }
        }

        if set_clauses.is_empty() {
            return Err(SqlCompileError::EmptyMutation("batch_update".to_owned()));
        }

        let mut in_placeholders = Vec::new();
        for id in &command.batch_ids {
            params.push(id.clone());
            in_placeholders.push(self.placeholder(params.len()));
        }
        let mut predicates = vec![format!(
            "{} IN ({})",
            self.quote_ident(&id_property.column_name),
            in_placeholders.join(", ")
        )];

        if has_versions {
            let version_property = entity.version_property().unwrap();
            let mut case_parts = Vec::new();
            case_parts.push(format!(
                "CASE {}",
                self.quote_ident(&id_property.column_name)
            ));

            for (i, exp_ver_opt) in command.batch_expected_versions.iter().enumerate() {
                if let Some(exp_ver) = exp_ver_opt {
                    let id = &command.batch_ids[i];

                    params.push(id.clone());
                    let id_ph = self.placeholder(params.len());

                    params.push(teaql_core::Value::I64(*exp_ver));
                    let val_ph = self.placeholder(params.len());

                    case_parts.push(format!("WHEN {} THEN {}", id_ph, val_ph));
                }
            }
            case_parts.push(format!(
                "ELSE {} END",
                self.quote_ident(&version_property.column_name)
            ));

            predicates.push(format!(
                "{} = {}",
                self.quote_ident(&version_property.column_name),
                case_parts.join(" ")
            ));
        }

        Ok(CompiledQuery {
            sql: format!(
                "UPDATE {} SET {} WHERE {}",
                self.quote_ident(&entity.table_name),
                set_clauses.join(", "),
                predicates.join(" AND ")
            ),
            params,
            comment: None,
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
                comment: None,
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
            comment: None,
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
            comment: None,
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
        let field = self.resolve_order_field(entity, order_by, params)?;
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
        let property_projection = |property: &PropertyDescriptor| self.column_with_alias(property);

        if query.projection.is_empty()
            && query.expr_projection.is_empty()
            && query.raw_projections.is_empty()
            && query.dynamic_properties.is_empty()
        {
            return Ok(entity
                .properties
                .iter()
                .map(property_projection)
                .collect::<Vec<_>>()
                .join(", "));
        }
        // Generated relation selections may request an identity field both as
        // a base projection and as part of the selected entity graph. MySQL
        // rejects duplicate column names inside the partition/window derived
        // table, so preserve first-seen order while removing duplicates.
        let mut seen_fields = std::collections::BTreeSet::new();
        let mut parts = Vec::new();
        for field in &query.projection {
            if !seen_fields.insert(field.as_str()) {
                continue;
            }
            let property = entity
                .property_by_name(field)
                .ok_or_else(|| SqlCompileError::UnknownField(field.to_owned()))?;
            parts.push(property_projection(property));
        }
        for projection in &query.expr_projection {
            let expr = self.compile_expr(entity, &projection.expr, params)?;
            parts.push(format!("{expr} AS {}", self.quote_ident(&projection.alias)));
        }
        for projection in query
            .raw_projections
            .iter()
            .chain(query.dynamic_properties.iter())
        {
            parts.push(format!(
                "{} AS {}",
                projection.raw_sql_segment,
                self.quote_ident(&projection.property_name)
            ));
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
        // Aggregate queries must not inherit the entity's ordinary/default projection.
        // Only grouping keys may be projected alongside aggregate expressions;
        // otherwise generated requests produce invalid SQL such as
        // `SELECT id, COUNT(id) ...` without grouping by `id`.
        for field in &query.group_by {
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
        for projection in query
            .raw_projections
            .iter()
            .chain(query.dynamic_properties.iter())
        {
            let aliased = format!(
                "{} AS {}",
                projection.raw_sql_segment,
                self.quote_ident(&projection.property_name)
            );
            if !parts.contains(&aliased) {
                parts.push(aliased);
            }
        }
        parts.extend(
            query
                .aggregates
                .iter()
                .map(|aggregate| {
                    let field = self.resolve_aggregate_field(entity, aggregate)?;
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
            ExprFunction::Gbk => self.compile_gbk_function(entity, args, params),
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

    /// Compile a GBK sort expression. The default implementation returns an error
    /// because GBK encoding conversion is dialect-specific. PostgreSQL dialects
    /// should override this to use `convert_to(arg, 'GBK')`.
    fn compile_gbk_function(
        &self,
        entity: &EntityDescriptor,
        args: &[Expr],
        params: &mut Vec<Value>,
    ) -> Result<String, SqlCompileError> {
        let [arg] = args else {
            return Err(SqlCompileError::InvalidFunctionArguments(
                "GBK expects exactly one argument".to_owned(),
            ));
        };
        // Default: pass through the column as-is (no GBK conversion).
        // Dialects with GBK support (e.g. PostgreSQL) should override this method.
        let arg = self.compile_expr(entity, arg, params)?;
        Ok(arg)
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

    fn compile_projection(
        &self,
        entity: &EntityDescriptor,
        query: &SelectQuery,
        params: &mut Vec<Value>,
    ) -> Result<String, SqlCompileError> {
        match query.aggregates.is_empty() {
            true => self.select_projection(entity, query, params),
            false => self.aggregate_projection(entity, query, params),
        }
    }

    fn resolve_order_field(
        &self,
        entity: &EntityDescriptor,
        order_by: &OrderBy,
        params: &mut Vec<Value>,
    ) -> Result<String, SqlCompileError> {
        match &order_by.expr {
            Some(expr) => self.compile_expr(entity, expr, params),
            None => self.column_sql(entity, &order_by.field),
        }
    }

    fn column_with_alias(&self, property: &PropertyDescriptor) -> String {
        let column = self.quote_ident(&property.column_name);
        match property.column_name == property.name {
            true => column,
            false => format!("{column} AS {}", self.quote_ident(&property.name)),
        }
    }

    fn resolve_aggregate_field(
        &self,
        entity: &EntityDescriptor,
        aggregate: &Aggregate,
    ) -> Result<String, SqlCompileError> {
        match aggregate.function == AggregateFunction::Count && aggregate.field == "*" {
            true => Ok("*".to_owned()),
            false => self.column_sql(entity, &aggregate.field),
        }
    }
}

fn validate_schema_shape(property: &PropertyDescriptor) -> Result<(), SqlCompileError> {
    let invalid = |reason: String| SqlCompileError::InvalidSchemaShape {
        property: property.name.clone(),
        reason,
    };

    match property.data_type {
        DataType::Text => {
            if property.max_length == Some(0) {
                return Err(invalid("max_length must be greater than zero".to_owned()));
            }
        }
        _ if property.max_length.is_some() => {
            return Err(invalid(format!(
                "max_length is only valid for Text, not {:?}",
                property.data_type
            )));
        }
        _ => {}
    }

    match property.data_type {
        DataType::Decimal => match (property.numeric_precision, property.numeric_scale) {
            (None, None) => {}
            (Some(0), _) => {
                return Err(invalid(
                    "numeric_precision must be greater than zero".to_owned(),
                ));
            }
            (Some(precision), Some(scale)) if scale <= precision => {}
            (Some(precision), Some(scale)) => {
                return Err(invalid(format!(
                    "numeric_scale ({scale}) must not exceed numeric_precision ({precision})"
                )));
            }
            _ => {
                return Err(invalid(
                    "numeric_precision and numeric_scale must be specified together".to_owned(),
                ));
            }
        },
        _ if property.numeric_precision.is_some() || property.numeric_scale.is_some() => {
            return Err(invalid(format!(
                "numeric_precision/numeric_scale are only valid for Decimal, not {:?}",
                property.data_type
            )));
        }
        _ => {}
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DatabaseKind;
    use teaql_core::{DataType, EntityDescriptor, InsertCommand, PropertyDescriptor, Value};

    struct TestDialect;
    impl crate::SqlDialect for TestDialect {
        fn kind(&self) -> DatabaseKind {
            DatabaseKind::PostgreSql
        }
        fn quote_ident(&self, ident: &str) -> String {
            ident.to_owned()
        }
        fn placeholder(&self, index: usize) -> String {
            format!("${index}")
        }
        fn schema_type_sql(
            &self,
            _data_type: DataType,
            _property: &PropertyDescriptor,
        ) -> Result<String, SqlCompileError> {
            Ok("TEST".to_owned())
        }
    }

    #[test]
    fn test_regression_issue_56_typed_null_conversion() {
        let dialect = TestDialect;
        let mut entity = EntityDescriptor::new("User");
        entity
            .properties
            .push(PropertyDescriptor::new("paid_at", DataType::Timestamp));

        let mut command = InsertCommand::new("User");
        command = command.value("paid_at", Value::Null);

        let query = dialect.compile_insert(&entity, &command).unwrap();
        // The value should be converted to TypedNull(Timestamp)
        assert_eq!(query.params.len(), 1);
        assert_eq!(query.params[0], Value::TypedNull(DataType::Timestamp));
    }

    #[test]
    fn rejects_invalid_schema_shape_before_emitting_ddl() {
        let dialect = TestDialect;
        let mut entity = EntityDescriptor::new("Order").table_name("orders");

        for property in [
            PropertyDescriptor::new("empty_text", DataType::Text).max_length(0),
            PropertyDescriptor::new("partial_decimal", DataType::Decimal).numeric_precision(19),
            PropertyDescriptor::new("invalid_decimal", DataType::Decimal)
                .numeric_precision(5)
                .numeric_scale(7),
            PropertyDescriptor::new("numeric_text", DataType::Text)
                .numeric_precision(19)
                .numeric_scale(7),
        ] {
            entity.properties = vec![property.clone()];
            let error = dialect
                .compile_create_table(&entity)
                .expect_err("invalid metadata must fail before DDL");
            assert!(matches!(error, SqlCompileError::InvalidSchemaShape { .. }));
            assert!(error.to_string().contains(&property.name));
        }
    }

    #[test]
    fn existing_storage_shape_must_cover_the_model_value_domain() {
        assert!(storage_length_covers(100, Some(100)));
        assert!(storage_length_covers(100, Some(255)));
        assert!(storage_length_covers(100, None));
        assert!(!storage_length_covers(100, Some(32)));

        assert!(storage_numeric_covers(19, 7, Some(19), Some(7)));
        assert!(storage_numeric_covers(19, 7, Some(38), Some(10)));
        assert!(storage_numeric_covers(19, 7, None, None));
        assert!(!storage_numeric_covers(19, 7, Some(18), Some(2)));
        assert!(!storage_numeric_covers(19, 7, Some(19), Some(8)));
        assert!(!storage_numeric_covers(19, 7, Some(20), Some(6)));
        assert!(!storage_numeric_covers(19, 7, Some(38), None));
    }
}
