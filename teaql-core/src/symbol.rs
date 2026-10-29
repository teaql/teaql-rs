//! Generated, process-shared symbol identities. Database IDs are deliberately separate.

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntityTypeId(pub u32);

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FieldSlot(pub u32);

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelationSlot(pub u32);

/// One generation's dictionary; IDs are offsets, not stable wire identifiers.
#[derive(Debug)]
pub struct ModelSymbols {
    pub entities: &'static [&'static str],
}

#[derive(Debug)]
pub struct EntitySymbols {
    pub model: &'static ModelSymbols,
    pub entity: EntityTypeId,
    /// Storage field names in lexical order, preserving legacy mutation order.
    pub fields: &'static [&'static str],
    pub relations: &'static [&'static str],
}

impl EntitySymbols {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.name().is_none() {
            return Err("entity ID outside generated model");
        }
        if u32::try_from(self.fields.len()).is_err() || u32::try_from(self.relations.len()).is_err()
        {
            return Err("too many generated fields");
        }
        if self
            .fields
            .iter()
            .any(|name| name.is_empty() || name.starts_with(['#', '_']))
        {
            return Err("invalid fixed field name");
        }
        if self.fields.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err("generated fields must be unique and sorted");
        }
        if self.relations.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err("generated relations must be unique and sorted");
        }
        Ok(())
    }

    pub fn name(&self) -> Option<&'static str> {
        self.model.entities.get(self.entity.0 as usize).copied()
    }

    pub fn field(&self, slot: FieldSlot) -> Option<&'static str> {
        self.fields.get(slot.0 as usize).copied()
    }

    pub fn slot(&self, name: &str) -> Option<FieldSlot> {
        self.fields
            .binary_search(&name)
            .ok()
            .and_then(|i| u32::try_from(i).ok())
            .map(FieldSlot)
    }

    pub fn relation(&self, slot: RelationSlot) -> Option<&'static str> {
        self.relations.get(slot.0 as usize).copied()
    }
    pub fn relation_slot(&self, name: &str) -> Option<RelationSlot> {
        self.relations
            .binary_search(&name)
            .ok()
            .and_then(|i| u32::try_from(i).ok())
            .map(RelationSlot)
    }

    pub fn same_layout(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}

impl PartialEq for EntitySymbols {
    fn eq(&self, other: &Self) -> bool {
        self.same_layout(other)
    }
}
impl Eq for EntitySymbols {}
