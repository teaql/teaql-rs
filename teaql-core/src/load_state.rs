use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// Type-local optimization hints, not ownership of every live result shape.
const MAX_SHARED_STATE_HINTS: usize = 512;

/// Immutable type metadata. Positions originate in the generated library, never access order.
#[derive(Debug)]
pub struct FieldLayout {
    entity: Arc<str>,
    revision: String,
    names: Vec<String>,
    aliases: HashMap<String, usize>,
    relations: HashSet<String>,
    states: std::sync::Mutex<HashMap<u64, Vec<std::sync::Weak<LoadedSnapshot>>>>,
}

impl FieldLayout {
    pub fn from_generated(
        entity: &str,
        revision: &str,
        indexes: &[(&str, usize)],
        mappings: &[(&str, &str, &str)],
        relations: &[&str],
        expected_members: &[&str],
    ) -> Result<Arc<Self>, String> {
        if entity.is_empty() || revision.is_empty() {
            return Err("missing entity or field-layout revision".to_owned());
        }
        let mut names = vec![String::new(); indexes.len()];
        let mut aliases = HashMap::with_capacity(indexes.len() * 2);
        for &(name, index) in indexes {
            if name.is_empty() || name.starts_with(['#', '_']) || index >= names.len() {
                return Err(format!("invalid fixed field/index: {name}={index}"));
            }
            if !names[index].is_empty() || aliases.insert(name.to_owned(), index).is_some() {
                return Err(format!("duplicate fixed field/index: {name}={index}"));
            }
            names[index] = name.to_owned();
        }
        if !aliases.contains_key("id") || !aliases.contains_key("version") {
            return Err("generated layout must include id and version".to_owned());
        }
        let mut mapped = HashSet::new();
        for &(canonical, member, column) in mappings {
            let Some(&index) = aliases.get(canonical) else {
                return Err(format!("mapping has no fixed field: {canonical}"));
            };
            if names[index] != canonical {
                return Err(format!(
                    "mapping names an alias, not a canonical field: {canonical}"
                ));
            }
            if !mapped.insert(canonical) {
                return Err(format!("duplicate field mapping: {canonical}"));
            }
            for alias in [member, column] {
                if alias.is_empty() {
                    return Err(format!("blank alias for {canonical}"));
                }
                if aliases
                    .get(alias)
                    .is_some_and(|previous| *previous != index)
                {
                    return Err(format!("ambiguous fixed-field alias: {alias}"));
                }
                aliases.insert(alias.to_owned(), index);
            }
        }
        if mapped.len() != indexes.len() {
            return Err("incomplete fixed-field mappings".to_owned());
        }
        let expected: HashSet<_> = expected_members
            .iter()
            .map(|member| aliases.get(*member).copied())
            .collect();
        if expected.contains(&None)
            || expected.len() != indexes.len()
            || expected_members.len() != indexes.len()
        {
            return Err("generated layout disagrees with typed entity fields".to_owned());
        }
        Ok(Arc::new(Self {
            entity: Arc::from(entity),
            revision: revision.to_owned(),
            names,
            aliases,
            relations: relations.iter().map(|name| (*name).to_owned()).collect(),
            states: Default::default(),
        }))
    }

    pub fn entity(&self) -> &str {
        &self.entity
    }
    /// Framework snapshot construction: entity identity belongs to the type,
    /// not to each row's value payload or mutation ledger.
    #[doc(hidden)]
    pub fn shared_entity_name(&self) -> Arc<str> {
        self.entity.clone()
    }
    pub fn revision(&self) -> &str {
        &self.revision
    }
    pub fn index(&self, name: &str) -> Option<usize> {
        self.aliases.get(name).copied()
    }
    pub fn field_count(&self) -> usize {
        self.names.len()
    }
    pub fn is_relation(&self, name: &str) -> bool {
        self.relations.contains(name)
    }
}

/// Value-free availability shared by compatible rows. Overflow and named selections are lazy.
#[derive(Debug, Clone)]
pub struct LoadedSnapshot {
    layout: Arc<FieldLayout>,
    bits: u64,
    overflow: Option<Arc<HashSet<usize>>>,
    selected_names: Option<Arc<HashSet<String>>>,
}

impl LoadedSnapshot {
    /// Intern immutable geometry only. Bounded weak hints never own row values.
    pub fn into_shared(self) -> Arc<Self> {
        use std::hash::{Hash, Hasher};
        fn unordered_hash<T: Hash>(values: impl Iterator<Item = T>) -> u64 {
            values.fold(0_u64, |total, value| {
                let mut hash = std::collections::hash_map::DefaultHasher::new();
                value.hash(&mut hash);
                total.wrapping_add(hash.finish())
            })
        }
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        self.bits.hash(&mut hash);
        unordered_hash(self.overflow.iter().flat_map(|fields| fields.iter())).hash(&mut hash);
        unordered_hash(self.selected_names.iter().flat_map(|fields| fields.iter())).hash(&mut hash);
        let key = hash.finish();
        let layout = self.layout.clone();
        let mut states = layout
            .states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = states
            .get(&key)
            .into_iter()
            .flatten()
            .filter_map(std::sync::Weak::upgrade)
            .find(|state| **state == self)
        {
            return existing;
        }
        // A live hit must not scan every historical projection. On a miss,
        // reclaim dead hints and count references (including hash collisions).
        let mut retained = 0;
        states.retain(|_, bucket| {
            bucket.retain(|state| state.strong_count() > 0);
            retained += bucket.len();
            !bucket.is_empty()
        });
        let state = Arc::new(self);
        if retained < MAX_SHARED_STATE_HINTS {
            states.entry(key).or_default().push(Arc::downgrade(&state));
        }
        // Saturation affects cache admission only. Operation-local result
        // layouts still share this state; held views remain fully valid.
        state
    }

    pub fn fully_loaded(layout: Arc<FieldLayout>) -> Self {
        let count = layout.field_count();
        Self {
            layout,
            bits: if count >= 64 {
                u64::MAX
            } else {
                (1_u64 << count) - 1
            },
            overflow: (count > 64).then(|| Arc::new((64..count).collect())),
            selected_names: None,
        }
    }
    pub fn projection<'a>(
        layout: Arc<FieldLayout>,
        fields: impl IntoIterator<Item = &'a str>,
    ) -> Self {
        let mut state = Self {
            layout,
            bits: 0,
            overflow: None,
            selected_names: None,
        };
        for field in fields {
            state.mark(field, true);
        }
        state
    }

    pub fn layout(&self) -> &Arc<FieldLayout> {
        &self.layout
    }
    pub fn bits(&self) -> u64 {
        self.bits
    }
    pub fn overflow(&self) -> Option<&HashSet<usize>> {
        self.overflow.as_deref()
    }
    pub fn selected_names(&self) -> Option<&HashSet<String>> {
        self.selected_names.as_deref()
    }

    /// Replace only persistent dynamic selection. Caller batches by actual shape;
    /// fixed bits, overflow and relation materialization are left untouched.
    pub fn with_dynamic_fields(
        state: &Arc<Self>,
        values: &crate::dynamic_fields::DynamicFieldValues,
    ) -> Result<Arc<Self>, String> {
        if values.definitions().owner_type() != state.layout.entity() {
            return Err("dynamic field owner disagrees with fixed entity layout".to_owned());
        }
        // Value-only edits do not change availability. Compare borrowed codes before
        // building an owned set or formatting prefixed names; relation markers remain
        // outside this comparison and must survive genuine dynamic-selection changes.
        let selected = values.selected_codes();
        let mut existing_count = 0;
        let same_selection = state
            .selected_names
            .iter()
            .flat_map(|names| names.iter())
            .filter_map(|name| name.strip_prefix('#'))
            .all(|code| {
                existing_count += 1;
                selected.contains(code)
            });
        if same_selection && existing_count == selected.len() {
            return Ok(state.clone());
        }
        let mut names: HashSet<String> = state
            .selected_names
            .iter()
            .flat_map(|names| names.iter())
            .filter(|name| !name.starts_with('#'))
            .cloned()
            .collect();
        names.extend(
            values
                .selected_codes()
                .iter()
                .map(|code| format!("#{code}")),
        );
        let mut next = (**state).clone();
        next.selected_names = (!names.is_empty()).then(|| Arc::new(names));
        Ok(next.into_shared())
    }

    pub fn is_loaded(&self, field: &str) -> bool {
        if self.layout.is_relation(field) || field.starts_with('#') {
            return self
                .selected_names
                .as_ref()
                .is_some_and(|names| names.contains(field));
        }
        match self.layout.index(field) {
            Some(index) if index < u64::BITS as usize => self.bits & (1_u64 << index) != 0,
            Some(index) => self
                .overflow
                .as_ref()
                .is_some_and(|fields| fields.contains(&index)),
            None => false, // Derived properties and unknown aliases never acquire fixed slots.
        }
    }

    pub fn with_loaded(state: &Arc<Self>, field: &str, loaded: bool) -> Result<Arc<Self>, String> {
        if state.layout.index(field).is_none()
            && !state.layout.is_relation(field)
            && !field.starts_with('#')
        {
            return Err(format!(
                "unknown loadable field: {}.{field}",
                state.layout.entity()
            ));
        }
        if state.is_loaded(field) == loaded {
            return Ok(state.clone());
        }
        let mut next = (**state).clone();
        next.mark(field, loaded);
        Ok(next.into_shared())
    }

    fn mark(&mut self, field: &str, loaded: bool) {
        if self.layout.is_relation(field) || field.starts_with('#') {
            if self.is_loaded(field) == loaded {
                return;
            }
            let names = self
                .selected_names
                .get_or_insert_with(|| Arc::new(HashSet::new()));
            if loaded {
                Arc::make_mut(names).insert(field.to_owned());
            } else {
                Arc::make_mut(names).remove(field);
            }
            if names.is_empty() {
                self.selected_names = None;
            }
        } else if let Some(index) = self.layout.index(field) {
            if index < u64::BITS as usize {
                if loaded {
                    self.bits |= 1_u64 << index;
                } else {
                    self.bits &= !(1_u64 << index);
                }
            } else {
                if self.is_loaded(field) == loaded {
                    return;
                }
                let fields = self
                    .overflow
                    .get_or_insert_with(|| Arc::new(HashSet::new()));
                if loaded {
                    Arc::make_mut(fields).insert(index);
                } else {
                    Arc::make_mut(fields).remove(&index);
                }
                if fields.is_empty() {
                    self.overflow = None;
                }
            }
        }
    }
}

impl PartialEq for LoadedSnapshot {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.layout, &other.layout)
            && self.bits == other.bits
            && self.overflow == other.overflow
            && self.selected_names == other.selected_names
    }
}
impl Eq for LoadedSnapshot {}

#[cfg(test)]
mod lifetime_tests {
    use super::*;

    #[test]
    fn live_projection_cache_is_bounded_and_saturated_lists_still_share_geometry() {
        let layout = FieldLayout::from_generated(
            "BoundedProbe",
            "v1",
            &[("id", 0), ("version", 1), ("name", 2)],
            &[
                ("id", "id", "id"),
                ("version", "version", "version"),
                ("name", "name", "name"),
            ],
            &[],
            &["id", "version", "name"],
        )
        .unwrap();
        let retained = LoadedSnapshot::projection(layout.clone(), ["id", "name"]).into_shared();
        let mut held = Vec::new();
        for index in 0..10_000 {
            let code = format!("#held_{index}");
            let state =
                LoadedSnapshot::projection(layout.clone(), ["id", code.as_str()]).into_shared();
            assert!(state.is_loaded(&code));
            assert!(!retained.is_loaded(&code));
            held.push(state);
        }
        let cache = layout.states.lock().unwrap();
        assert!(
            cache.values().map(Vec::len).sum::<usize>() <= 512,
            "live result shapes must not make the type cache unbounded"
        );
        drop(cache);
        let reused = LoadedSnapshot::projection(layout.clone(), ["id", "name"]).into_shared();
        assert!(Arc::ptr_eq(&retained, &reused));
        // One provider/result layout owns the actual list shape even when
        // the optional type-level interner has exhausted its admission budget.
        let columns = crate::CompactRowLayout::new(Arc::from([
            "id".to_owned(),
            "version".to_owned(),
            "name".to_owned(),
        ]));
        let mut first = None;
        for id in 0..10_000 {
            let row = crate::CompactRow::with_layout(
                columns.clone(),
                vec![
                    crate::Value::U64(id),
                    crate::Value::I64(1),
                    crate::Value::Null,
                ],
            );
            let crate::eval::LoadState::Indexed(state) = row.indexed_load_state(layout.clone())
            else {
                panic!("indexed layout required");
            };
            assert!(state.is_loaded("name"));
            assert_eq!(row.get("id"), Some(&crate::Value::U64(id)));
            if let Some(first) = &first {
                assert!(Arc::ptr_eq(first, &state));
            } else {
                first = Some(state);
            }
        }
        drop(columns);
        drop(held);
        assert!(first.unwrap().is_loaded("name"));
        assert!(!retained.is_loaded("version"));
    }

    #[test]
    fn dead_projection_entries_are_pruned_without_retaining_states_or_layouts() {
        let layout = FieldLayout::from_generated(
            "LifetimeProbe",
            "v1",
            &[("id", 0), ("version", 1), ("name", 2)],
            &[
                ("id", "id", "id"),
                ("version", "version", "version"),
                ("name", "name", "name"),
            ],
            &[],
            &["id", "version", "name"],
        )
        .unwrap();
        let weak_layout = Arc::downgrade(&layout);
        let retained = LoadedSnapshot::projection(layout.clone(), ["id", "name"]).into_shared();
        let weak_retained = Arc::downgrade(&retained);
        for index in 0..1_000 {
            let code = format!("#temporary_{index}");
            let temporary =
                LoadedSnapshot::projection(layout.clone(), ["id", code.as_str()]).into_shared();
            let weak_temporary = Arc::downgrade(&temporary);
            assert!(temporary.is_loaded(&code));
            assert!(!retained.is_loaded(&code));
            drop(temporary);
            assert!(weak_temporary.upgrade().is_none());
        }
        // A live hit leaves unrelated weak hints alone; the final dead hint
        // owns no snapshot fields and will be swept by the next miss.
        let reused = LoadedSnapshot::projection(layout.clone(), ["id", "name"]).into_shared();
        assert!(Arc::ptr_eq(&retained, &reused));
        let cache = layout.states.lock().unwrap();
        assert!(cache.len() <= 2);
        assert!(cache.values().map(Vec::len).sum::<usize>() <= 2);
        drop(cache);
        drop(reused);
        drop(layout);
        assert!(weak_layout.upgrade().is_some());
        assert!(retained.is_loaded("name"));
        assert!(!retained.is_loaded("version"));
        drop(retained);
        assert!(weak_retained.upgrade().is_none());
        assert!(weak_layout.upgrade().is_none());
    }
}
