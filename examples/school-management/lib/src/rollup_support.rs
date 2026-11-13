//! Internal live parent configurations. No database or Context is accessed here.
use std::{collections::{BTreeMap, BTreeSet}, sync::{Arc, Mutex}};
use teaql_core::EntityDescriptor;
use teaql_core::request::{ObjectGroupBy, QuerySelection};
use teaql_runtime::RuntimeError;

pub(crate) type RollupTarget = Arc<Mutex<RollupState>>;

#[derive(Clone, Debug)]
pub(crate) struct RollupState {
    pub selection: QuerySelection,
    pub links: Vec<RollupLink>,
}

#[derive(Clone, Debug)]
pub(crate) struct RollupLink {
    pub property: String,
    pub storage: String,
    pub descriptor: EntityDescriptor,
    pub target: RollupTarget,
}

/// Freeze each shared target once for this execution, without holding nested locks.
pub(crate) fn resolve_rollup_links(
    source: QuerySelection,
    links: &[RollupLink],
) -> Result<QuerySelection, RuntimeError> {
    Resolver::default().apply(source, links)
}

pub(crate) fn reaches(target: &RollupTarget, needle: &RollupTarget) -> Result<bool, RuntimeError> {
    let mut pending = vec![target.clone()];
    let mut visited = BTreeSet::new();
    while let Some(current) = pending.pop() {
        if Arc::ptr_eq(&current, needle) { return Ok(true); }
        if !visited.insert(Arc::as_ptr(&current) as usize) { continue; }
        let state = current.lock()
            .map_err(|_| RuntimeError::Graph("poisoned roll-up configuration".into()))?;
        pending.extend(state.links.iter().map(|link| link.target.clone()));
    }
    Ok(false)
}

#[derive(Default)]
struct Resolver {
    active: BTreeSet<usize>,
    snapshots: BTreeMap<usize, QuerySelection>,
}

impl Resolver {
    fn target(&mut self, target: &RollupTarget) -> Result<QuerySelection, RuntimeError> {
        let key = Arc::as_ptr(target) as usize;
        if let Some(snapshot) = self.snapshots.get(&key) { return Ok(snapshot.clone()); }
        if self.active.len() >= 64 || !self.active.insert(key) {
            return Err(RuntimeError::Graph("cyclic or excessively deep roll-up configuration".into()));
        }
        let state = target.lock()
            .map_err(|_| RuntimeError::Graph("poisoned roll-up configuration".into()))?.clone();
        let selection = self.apply(state.selection, &state.links)?;
        self.active.remove(&key);
        self.snapshots.insert(key, selection.clone());
        Ok(selection)
    }

    fn apply(&mut self, mut source: QuerySelection, links: &[RollupLink])
        -> Result<QuerySelection, RuntimeError> {
        for link in links {
            let parent = self.target(&link.target)?;
            if parent.query.entity != link.descriptor.name {
                return Err(RuntimeError::Graph("roll-up target entity does not match its relation".into()));
            }
            let statistical = !parent.query.group_by.is_empty() || !parent.query.aggregates.is_empty()
                || parent.query.has_aggregate_projection()
                || !parent.query_options.object_group_bys.is_empty();
            if !statistical {
                source.query = source.query.and_filter(teaql_core::request::rollup_navigation_membership(
                    &link.property, &link.storage, link.descriptor.clone(), parent.clone().into_query()));
            }
            if !source.query.group_by.contains(&link.storage) {
                source.query = source.query.group_by(&link.storage);
            }
            source.query_options.object_group_bys.retain(|group| group.property_name != link.property);
            source.query_options.object_group_bys.push(ObjectGroupBy::new(
                &link.property, &link.storage, parent));
        }
        Ok(source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use teaql_core::SelectQuery;

    fn target(entity: &str) -> RollupTarget {
        Arc::new(Mutex::new(RollupState {
            selection: QuerySelection::new(SelectQuery::new(entity)), links: Vec::new(),
        }))
    }

    fn link(target: &RollupTarget) -> RollupLink {
        RollupLink { property: "parent".into(), storage: "parent_id".into(),
            descriptor: EntityDescriptor::new("Parent"), target: target.clone() }
    }

    #[test]
    fn ordinary_selection_has_no_shared_state_requirement() {
        let selection = QuerySelection::new(SelectQuery::new("Child"));
        assert_eq!(resolve_rollup_links(selection.clone(), &[]).unwrap(), selection);
    }

    #[test]
    fn repeated_target_is_frozen_once_per_resolution() {
        let target = target("Parent");
        let mut resolver = Resolver::default();
        let first = resolver.target(&target).unwrap();
        target.lock().unwrap().selection.query = SelectQuery::new("Changed");
        assert_eq!(resolver.target(&target).unwrap(), first);
        assert_eq!(Resolver::default().target(&target).unwrap().query.entity, "Changed");
    }

    #[test]
    fn cycles_fail_before_query_execution() {
        let target = target("Parent");
        target.lock().unwrap().links.push(link(&target));
        let result = resolve_rollup_links(QuerySelection::new(SelectQuery::new("Child")), &[link(&target)]);
        // Break the intentionally malformed private test graph before assertions.
        target.lock().unwrap().links.clear();
        assert!(result.unwrap_err().to_string().contains("cyclic"));
    }

    #[test]
    fn grouped_parent_is_never_used_as_an_identity_subquery() {
        let target = target("Parent");
        target.lock().unwrap().selection.query = SelectQuery::new("Parent").group_by("platform_id");
        let result = resolve_rollup_links(QuerySelection::new(SelectQuery::new("Child")), &[link(&target)]);
        let selection = result.unwrap();
        assert!(selection.query.filter.is_none());
        assert!(selection.relation_filters.is_empty());
        assert_eq!(selection.query_options.object_group_bys.len(), 1);
        target.lock().unwrap().selection.query = SelectQuery::new("Parent")
            .project_expr("count", teaql_core::Expr::count_all());
        let selection = resolve_rollup_links(QuerySelection::new(SelectQuery::new("Child")), &[link(&target)]).unwrap();
        assert!(selection.query.filter.is_none());
        assert_eq!(selection.query_options.object_group_bys.len(), 1);
    }

    #[test]
    fn mismatched_parent_is_rejected() {
        let target = target("AnotherType");
        let result = resolve_rollup_links(QuerySelection::new(SelectQuery::new("Child")), &[link(&target)]);
        assert!(result.unwrap_err().to_string().contains("does not match"));
    }

    #[test]
    fn independent_relation_filter_metadata_survives_navigation() {
        let target = target("Parent");
        let mut source = QuerySelection::new(SelectQuery::new("Child"));
        source.relation_filters.push(teaql_core::request::RelationFilter::new(
            "parent", SelectQuery::new("Parent").filter(teaql_core::Expr::eq("id", 7_i64))));
        let expected = source.relation_filters.clone();
        let resolved = resolve_rollup_links(source, &[link(&target)]).unwrap();
        assert_eq!(resolved.relation_filters, expected);
    }
}