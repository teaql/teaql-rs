//! Joined inheritance is a logical population, not duplicated parent values.
use crate::{SqlCompileError, SqlDialect};
use teaql_core::EntityDescriptor;

/// Root first, with bounded, compatible metadata. This is shared by schema,
/// reads and writes so none can silently interpret the same model differently.
pub(crate) fn hierarchy(
    entity: &EntityDescriptor,
) -> Result<Vec<&EntityDescriptor>, SqlCompileError> {
    let mut chain = Vec::new();
    let mut current = entity;
    loop {
        if chain.len() >= 64
            || chain.iter().any(|node: &&EntityDescriptor| {
                node.name == current.name || node.table_name == current.table_name
            })
        {
            return Err(invalid("cyclic, repeated or excessively deep inheritance"));
        }
        chain.push(current);
        let Some(parent) = &current.joined_parent else {
            break;
        };
        let route =
            |node: &EntityDescriptor| node.data_service.as_deref().unwrap_or("default").to_owned();
        if route(current) != route(parent) {
            return Err(invalid("joined inheritance cannot cross data services"));
        }
        for property in &parent.properties {
            if current.property_by_name(&property.name) != Some(property) {
                return Err(invalid(&format!(
                    "incompatible inherited field {}.{}",
                    current.name, property.name
                )));
            }
        }
        current = parent;
    }
    chain.reverse();
    if chain.len() > 1 {
        if chain[0].id_property().is_none() || chain[0].version_property().is_none() {
            return Err(invalid(
                "joined root requires identity and optimistic version",
            ));
        }
        for node in &chain {
            let mut names = std::collections::HashSet::new();
            if node
                .properties
                .iter()
                .any(|property| !names.insert(&property.column_name))
            {
                return Err(invalid("inherited SQL field mappings must be unambiguous"));
            }
        }
    }
    Ok(chain)
}

fn invalid(message: &str) -> SqlCompileError {
    SqlCompileError::InvalidFunctionArguments(format!("joined inheritance: {message}"))
}

/// Existing flattened child tables need an explicit migration. Merely adding
/// ancestor tables would leave old rows invisible and duplicated values stale.
pub fn validate_inherited_storage_columns<'a>(
    entity: &EntityDescriptor,
    columns: impl IntoIterator<Item = &'a str>,
) -> Result<(), SqlCompileError> {
    let Some(parent) = &entity.joined_parent else {
        return Ok(());
    };
    for column in columns {
        if parent
            .properties
            .iter()
            .any(|property| !property.is_id && property.column_name.eq_ignore_ascii_case(column))
        {
            return Err(invalid(&format!(
                "{} table {} contains inherited column {}; migrate flattened inherited storage explicitly before ensure_schema; no data was rewritten",
                entity.name, entity.table_name, column,
            )));
        }
    }
    Ok(())
}

/// Translate complete logical descriptors to declaring-table schemas. Ancestors
/// are included even when a caller registers only a leaf. The synthetic join FK
/// is schema-only and never becomes an application relation or a graph node.
pub fn physical_entity_descriptors(
    entities: &[&EntityDescriptor],
) -> Result<Vec<EntityDescriptor>, SqlCompileError> {
    let mut result: Vec<EntityDescriptor> = Vec::new();
    for entity in entities {
        for node in hierarchy(entity)? {
            let mut physical = node.storage_descriptor();
            if let Some(parent) = &node.joined_parent {
                physical.relations.push(
                    teaql_core::RelationDescriptor::new("__teaql_inheritance_parent", &parent.name)
                        .local_key(&node.id_property().expect("validated identity").name)
                        .foreign_key(&parent.id_property().expect("validated identity").name),
                );
            }
            if let Some(previous) = result
                .iter()
                .find(|candidate| candidate.name == physical.name)
            {
                if previous != &physical {
                    return Err(invalid("conflicting descriptors for one inheritance table"));
                }
            } else {
                result.push(physical);
            }
        }
    }
    Ok(result)
}

pub(crate) fn source<D: SqlDialect + ?Sized>(
    dialect: &D,
    entity: &EntityDescriptor,
) -> Result<String, SqlCompileError> {
    if entity.joined_parent.is_none() {
        return Ok(dialect.quote_ident(&entity.table_name));
    }
    let chain = hierarchy(entity)?;
    let q = |name: &str| dialect.quote_ident(name);
    let alias = |index: usize| q(&format!("__teaql_inherit_{index}"));
    let root_id = chain[0].id_property().expect("validated identity");
    let mut from = format!("{} AS {}", q(&chain[0].table_name), alias(0));
    for (index, node) in chain.iter().enumerate().skip(1) {
        let id = node.id_property().expect("validated inherited identity");
        from.push_str(&format!(
            " JOIN {} AS {} ON {}.{} = {}.{}",
            q(&node.table_name),
            alias(index),
            alias(index),
            q(&id.column_name),
            alias(0),
            q(&root_id.column_name)
        ));
    }
    let projection = entity
        .properties
        .iter()
        .map(|property| {
            let index = chain
                .iter()
                .position(|node| node.property_by_name(&property.name).is_some())
                .expect("logical field has a declaring table");
            format!(
                "{}.{} AS {}",
                alias(index),
                q(&property.column_name),
                q(&property.column_name)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "(SELECT {projection} FROM {from}) AS {}",
        q("__teaql_inherited")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use teaql_core::{DataType, PropertyDescriptor};
    struct Dialect;
    impl SqlDialect for Dialect {
        fn kind(&self) -> crate::DatabaseKind {
            crate::DatabaseKind::Sqlite
        }
        fn quote_ident(&self, name: &str) -> String {
            format!("\"{}\"", name.replace('"', "\"\""))
        }
        fn placeholder(&self, _: usize) -> String {
            "?".into()
        }
    }
    fn root() -> EntityDescriptor {
        EntityDescriptor::new("School")
            .property(PropertyDescriptor::new("id", DataType::U64).id())
            .property(PropertyDescriptor::new("version", DataType::I64).version())
            .property(PropertyDescriptor::new("name", DataType::Text).column_name("Legacy Name"))
    }
    #[test]
    fn derived_population_reads_each_value_from_its_declaring_table() {
        let child = EntityDescriptor::new("Academy")
            .inherits(root())
            .property(PropertyDescriptor::new("campus", DataType::Text));
        let leaf = EntityDescriptor::new("TrainingAcademy")
            .inherits(child)
            .property(PropertyDescriptor::new("specialty", DataType::Text));
        let sql = source(&Dialect, &leaf).unwrap();
        assert!(sql.contains("\"__teaql_inherit_0\".\"Legacy Name\" AS \"Legacy Name\""));
        assert!(sql.contains("\"__teaql_inherit_1\".\"campus\" AS \"campus\""));
        assert!(sql.contains("\"__teaql_inherit_2\".\"specialty\" AS \"specialty\""));
        assert_eq!(sql.matches(" JOIN ").count(), 2);
    }
    #[test]
    fn invalid_inheritance_never_reaches_sql_execution() {
        let repeated = EntityDescriptor::new("School").inherits(root());
        assert!(source(&Dialect, &repeated).is_err());
        let different_route = EntityDescriptor::new("Academy")
            .data_service("other")
            .inherits(root());
        assert!(source(&Dialect, &different_route).is_err());
        let mut renamed = EntityDescriptor::new("Academy").inherits(root());
        renamed.properties[2].column_name = "wrong".into();
        assert!(source(&Dialect, &renamed).is_err());
    }
}
