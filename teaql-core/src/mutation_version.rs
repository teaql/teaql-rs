//! Deterministic version transitions shared by compilers and execution paths.
//! Exhaustion is an error, never saturation, wraparound or a new lifecycle state.
use crate::MutationKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationVersionError {
    InvalidUpdate(i64),
    InvalidDelete(i64),
    InvalidRecovery(i64),
    Exhausted {
        operation: MutationKind,
        version: i64,
    },
}

impl MutationVersionError {
    pub fn version(self) -> i64 {
        match self {
            Self::InvalidUpdate(version)
            | Self::InvalidDelete(version)
            | Self::InvalidRecovery(version)
            | Self::Exhausted { version, .. } => version,
        }
    }
}

impl std::fmt::Display for MutationVersionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidUpdate(version) => write!(
                f,
                "update requires a positive original version, got {version}"
            ),
            Self::InvalidDelete(version) => write!(
                f,
                "soft deletion requires a positive original version, got {version}; use explicit recovery for a tombstone"
            ),
            Self::InvalidRecovery(version) => write!(
                f,
                "recovery requires a negative original version, got {version}"
            ),
            Self::Exhausted { operation, version } => write!(
                f,
                "{operation:?} cannot advance exhausted version {version} within signed 64-bit range"
            ),
        }
    }
}
impl std::error::Error for MutationVersionError {}

pub fn updated(version: i64) -> Result<i64, MutationVersionError> {
    if version <= 0 {
        return Err(MutationVersionError::InvalidUpdate(version));
    }
    version
        .checked_add(1)
        .ok_or(MutationVersionError::Exhausted {
            operation: MutationKind::Update,
            version,
        })
}

pub fn deleted(version: i64) -> Result<i64, MutationVersionError> {
    if version <= 0 {
        return Err(MutationVersionError::InvalidDelete(version));
    }
    // Every positive i64 has a representable tombstone, including MAX -> MIN.
    Ok(-version - 1)
}

pub fn recovered(version: i64) -> Result<i64, MutationVersionError> {
    if version >= 0 {
        return Err(MutationVersionError::InvalidRecovery(version));
    }
    version
        .checked_neg()
        .and_then(|value| value.checked_add(1))
        .ok_or(MutationVersionError::Exhausted {
            operation: MutationKind::Recover,
            version,
        })
}

#[cfg(test)]
mod version_lifecycle_tests {
    use super::*;
    #[test]
    fn version_lifecycle_checked_update_domain_and_adjacent_boundary() {
        for version in [i64::MIN, -902, -1, 0] {
            assert_eq!(
                updated(version),
                Err(MutationVersionError::InvalidUpdate(version))
            );
        }
        assert_eq!(updated(1), Ok(2));
        assert_eq!(updated(i64::MAX - 1), Ok(i64::MAX));
        assert_eq!(
            updated(i64::MAX),
            Err(MutationVersionError::Exhausted {
                operation: MutationKind::Update,
                version: i64::MAX
            })
        );
    }
    #[test]
    fn version_lifecycle_checked_delete_keeps_the_full_live_domain() {
        for version in [i64::MIN, -902, -1, 0] {
            assert_eq!(
                deleted(version),
                Err(MutationVersionError::InvalidDelete(version))
            );
        }
        assert_eq!(deleted(1), Ok(-2));
        assert_eq!(deleted(i64::MAX), Ok(i64::MIN));
    }
    #[test]
    fn version_lifecycle_checked_recovery_domain_and_adjacent_boundary() {
        for version in [0, 1, i64::MAX] {
            assert_eq!(
                recovered(version),
                Err(MutationVersionError::InvalidRecovery(version))
            );
        }
        for version in [i64::MIN, i64::MIN + 1] {
            assert_eq!(
                recovered(version),
                Err(MutationVersionError::Exhausted {
                    operation: MutationKind::Recover,
                    version
                })
            );
        }
        assert_eq!(recovered(-1), Ok(2));
        assert_eq!(recovered(-2), Ok(3));
        assert_eq!(recovered(i64::MIN + 2), Ok(i64::MAX));
    }
}
