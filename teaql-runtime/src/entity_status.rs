/// Actions that can be applied to an entity to transition its status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntityAction {
    Update,
    Delete,
    Persist,
    Recover,
}

/// Tracks the lifecycle status of an entity through a state machine.
///
/// State transitions follow the Java TeaQL transition table exactly:
///
/// | Current State     | Action  | Next State        |
/// |-------------------|---------|-------------------|
/// | New               | Update  | New               |
/// | New               | Persist | Persisted         |
/// | Persisted         | Update  | Updated           |
/// | Persisted         | Delete  | UpdatedDeleted    |
/// | PersistedDeleted  | Recover | UpdatedRecover    |
/// | Updated           | Update  | Updated           |
/// | Updated           | Persist | Persisted         |
/// | UpdatedDeleted    | Persist | PersistedDeleted  |
/// | UpdatedDeleted    | Delete  | UpdatedDeleted    |
/// | UpdatedRecover    | Persist | Persisted         |
/// | UpdatedRecover    | Recover | UpdatedRecover    |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntityStatus {
    /// A newly created entity that has not been persisted yet.
    New,
    /// An entity that has been persisted to the database.
    Persisted,
    /// A persisted entity that has been soft-deleted.
    PersistedDeleted,
    /// A persisted entity with pending updates.
    Updated,
    /// A persisted entity that has been updated and then deleted.
    UpdatedDeleted,
    /// A deleted entity that has been recovered (undeleted).
    UpdatedRecover,
    /// A reference to an entity managed elsewhere.
    Refer,
}

impl Default for EntityStatus {
    fn default() -> Self {
        EntityStatus::New
    }
}

impl EntityStatus {
    /// Transition to the next status given an action.
    ///
    /// Returns `Err` with a descriptive message for invalid transitions.
    pub fn next(self, action: EntityAction) -> Result<EntityStatus, String> {
        match (self, action) {
            // New
            (EntityStatus::New, EntityAction::Update) => Ok(EntityStatus::New),
            (EntityStatus::New, EntityAction::Persist) => Ok(EntityStatus::Persisted),

            // Persisted
            (EntityStatus::Persisted, EntityAction::Update) => Ok(EntityStatus::Updated),
            (EntityStatus::Persisted, EntityAction::Delete) => Ok(EntityStatus::UpdatedDeleted),

            // PersistedDeleted
            (EntityStatus::PersistedDeleted, EntityAction::Recover) => {
                Ok(EntityStatus::UpdatedRecover)
            }

            // Updated
            (EntityStatus::Updated, EntityAction::Update) => Ok(EntityStatus::Updated),
            (EntityStatus::Updated, EntityAction::Persist) => Ok(EntityStatus::Persisted),

            // UpdatedDeleted
            (EntityStatus::UpdatedDeleted, EntityAction::Persist) => {
                Ok(EntityStatus::PersistedDeleted)
            }
            (EntityStatus::UpdatedDeleted, EntityAction::Delete) => {
                Ok(EntityStatus::UpdatedDeleted)
            }

            // UpdatedRecover
            (EntityStatus::UpdatedRecover, EntityAction::Persist) => Ok(EntityStatus::Persisted),
            (EntityStatus::UpdatedRecover, EntityAction::Recover) => {
                Ok(EntityStatus::UpdatedRecover)
            }

            // All other combinations are invalid
            (status, action) => Err(format!(
                "invalid entity status transition: {:?} + {:?}",
                status, action
            )),
        }
    }

    /// Returns `true` if this entity needs to be persisted (i.e. it has pending changes).
    ///
    /// Entities with status `New`, `Updated`, `UpdatedDeleted`, or `UpdatedRecover`
    /// need persistence. `Persisted`, `PersistedDeleted`, and `Refer` do not.
    pub fn need_persist(&self) -> bool {
        matches!(
            self,
            EntityStatus::New
                | EntityStatus::Updated
                | EntityStatus::UpdatedDeleted
                | EntityStatus::UpdatedRecover
        )
    }

    /// Returns `true` if this entity is newly created and has never been persisted.
    pub fn is_new(&self) -> bool {
        matches!(self, EntityStatus::New)
    }

    /// Returns `true` if this entity has been updated since last persistence.
    pub fn is_updated(&self) -> bool {
        matches!(self, EntityStatus::Updated)
    }

    /// Returns `true` if this entity is marked for deletion.
    pub fn is_deleted(&self) -> bool {
        matches!(
            self,
            EntityStatus::UpdatedDeleted | EntityStatus::PersistedDeleted
        )
    }

    /// Returns `true` if this entity has been recovered from deletion.
    pub fn is_recover(&self) -> bool {
        matches!(self, EntityStatus::UpdatedRecover)
    }
}

impl std::fmt::Display for EntityStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EntityStatus::New => write!(f, "New"),
            EntityStatus::Persisted => write!(f, "Persisted"),
            EntityStatus::PersistedDeleted => write!(f, "PersistedDeleted"),
            EntityStatus::Updated => write!(f, "Updated"),
            EntityStatus::UpdatedDeleted => write!(f, "UpdatedDeleted"),
            EntityStatus::UpdatedRecover => write!(f, "UpdatedRecover"),
            EntityStatus::Refer => write!(f, "Refer"),
        }
    }
}

impl std::fmt::Display for EntityAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EntityAction::Update => write!(f, "Update"),
            EntityAction::Delete => write!(f, "Delete"),
            EntityAction::Persist => write!(f, "Persist"),
            EntityAction::Recover => write!(f, "Recover"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_new() {
        assert_eq!(EntityStatus::default(), EntityStatus::New);
    }

    #[test]
    fn new_update_stays_new() {
        assert_eq!(
            EntityStatus::New.next(EntityAction::Update).unwrap(),
            EntityStatus::New
        );
    }

    #[test]
    fn new_persist_becomes_persisted() {
        assert_eq!(
            EntityStatus::New.next(EntityAction::Persist).unwrap(),
            EntityStatus::Persisted
        );
    }

    #[test]
    fn persisted_update_becomes_updated() {
        assert_eq!(
            EntityStatus::Persisted.next(EntityAction::Update).unwrap(),
            EntityStatus::Updated
        );
    }

    #[test]
    fn persisted_delete_becomes_updated_deleted() {
        assert_eq!(
            EntityStatus::Persisted.next(EntityAction::Delete).unwrap(),
            EntityStatus::UpdatedDeleted
        );
    }

    #[test]
    fn persisted_deleted_recover_becomes_updated_recover() {
        assert_eq!(
            EntityStatus::PersistedDeleted
                .next(EntityAction::Recover)
                .unwrap(),
            EntityStatus::UpdatedRecover
        );
    }

    #[test]
    fn updated_update_stays_updated() {
        assert_eq!(
            EntityStatus::Updated.next(EntityAction::Update).unwrap(),
            EntityStatus::Updated
        );
    }

    #[test]
    fn updated_persist_becomes_persisted() {
        assert_eq!(
            EntityStatus::Updated.next(EntityAction::Persist).unwrap(),
            EntityStatus::Persisted
        );
    }

    #[test]
    fn updated_deleted_persist_becomes_persisted_deleted() {
        assert_eq!(
            EntityStatus::UpdatedDeleted
                .next(EntityAction::Persist)
                .unwrap(),
            EntityStatus::PersistedDeleted
        );
    }

    #[test]
    fn updated_deleted_delete_stays_updated_deleted() {
        assert_eq!(
            EntityStatus::UpdatedDeleted
                .next(EntityAction::Delete)
                .unwrap(),
            EntityStatus::UpdatedDeleted
        );
    }

    #[test]
    fn updated_recover_persist_becomes_persisted() {
        assert_eq!(
            EntityStatus::UpdatedRecover
                .next(EntityAction::Persist)
                .unwrap(),
            EntityStatus::Persisted
        );
    }

    #[test]
    fn updated_recover_recover_stays_updated_recover() {
        assert_eq!(
            EntityStatus::UpdatedRecover
                .next(EntityAction::Recover)
                .unwrap(),
            EntityStatus::UpdatedRecover
        );
    }

    #[test]
    fn invalid_transition_returns_err() {
        assert!(EntityStatus::New.next(EntityAction::Delete).is_err());
        assert!(EntityStatus::New.next(EntityAction::Recover).is_err());
        assert!(EntityStatus::Persisted.next(EntityAction::Persist).is_err());
        assert!(EntityStatus::Persisted.next(EntityAction::Recover).is_err());
        assert!(
            EntityStatus::PersistedDeleted
                .next(EntityAction::Update)
                .is_err()
        );
        assert!(
            EntityStatus::PersistedDeleted
                .next(EntityAction::Delete)
                .is_err()
        );
        assert!(
            EntityStatus::PersistedDeleted
                .next(EntityAction::Persist)
                .is_err()
        );
        assert!(EntityStatus::Updated.next(EntityAction::Delete).is_err());
        assert!(EntityStatus::Updated.next(EntityAction::Recover).is_err());
        assert!(
            EntityStatus::UpdatedDeleted
                .next(EntityAction::Update)
                .is_err()
        );
        assert!(
            EntityStatus::UpdatedDeleted
                .next(EntityAction::Recover)
                .is_err()
        );
        assert!(
            EntityStatus::UpdatedRecover
                .next(EntityAction::Update)
                .is_err()
        );
        assert!(
            EntityStatus::UpdatedRecover
                .next(EntityAction::Delete)
                .is_err()
        );
        assert!(EntityStatus::Refer.next(EntityAction::Update).is_err());
        assert!(EntityStatus::Refer.next(EntityAction::Delete).is_err());
        assert!(EntityStatus::Refer.next(EntityAction::Persist).is_err());
        assert!(EntityStatus::Refer.next(EntityAction::Recover).is_err());
    }

    #[test]
    fn need_persist_flags() {
        assert!(EntityStatus::New.need_persist());
        assert!(!EntityStatus::Persisted.need_persist());
        assert!(!EntityStatus::PersistedDeleted.need_persist());
        assert!(EntityStatus::Updated.need_persist());
        assert!(EntityStatus::UpdatedDeleted.need_persist());
        assert!(EntityStatus::UpdatedRecover.need_persist());
        assert!(!EntityStatus::Refer.need_persist());
    }

    #[test]
    fn helper_predicates() {
        assert!(EntityStatus::New.is_new());
        assert!(!EntityStatus::Persisted.is_new());

        assert!(EntityStatus::Updated.is_updated());
        assert!(!EntityStatus::New.is_updated());

        assert!(EntityStatus::UpdatedDeleted.is_deleted());
        assert!(EntityStatus::PersistedDeleted.is_deleted());
        assert!(!EntityStatus::Updated.is_deleted());

        assert!(EntityStatus::UpdatedRecover.is_recover());
        assert!(!EntityStatus::Updated.is_recover());
    }

    #[test]
    fn full_lifecycle_create_update_delete() {
        let status = EntityStatus::default();
        assert_eq!(status, EntityStatus::New);

        let status = status.next(EntityAction::Persist).unwrap();
        assert_eq!(status, EntityStatus::Persisted);

        let status = status.next(EntityAction::Update).unwrap();
        assert_eq!(status, EntityStatus::Updated);

        let status = status.next(EntityAction::Persist).unwrap();
        assert_eq!(status, EntityStatus::Persisted);

        let status = status.next(EntityAction::Delete).unwrap();
        assert_eq!(status, EntityStatus::UpdatedDeleted);

        let status = status.next(EntityAction::Persist).unwrap();
        assert_eq!(status, EntityStatus::PersistedDeleted);

        let status = status.next(EntityAction::Recover).unwrap();
        assert_eq!(status, EntityStatus::UpdatedRecover);

        let status = status.next(EntityAction::Persist).unwrap();
        assert_eq!(status, EntityStatus::Persisted);
    }
}
