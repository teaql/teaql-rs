//! Test-only boundary for frozen Result constructors and current direct entities.
//! Result keeps its inherent `expect`; direct entities must already own an ID.
use teaql_core::IdentifiableEntity;

fn require_reserved_id(id: u64, message: &str) {
    assert_ne!(id, 0, "{message}");
}

pub(crate) trait ReservedExampleIdentity: IdentifiableEntity + Sized {
    fn expect(self, message: &str) -> Self {
        require_reserved_id(self.id_value().try_u64().expect(message), message);
        self
    }
}
impl<T: IdentifiableEntity> ReservedExampleIdentity for T {}

#[test]
fn accepts_an_actual_reserved_identity() {
    require_reserved_id(1, "must reserve an example identity");
}

#[test]
#[should_panic(expected = "must reserve an example identity")]
fn rejects_an_unallocated_identity() {
    require_reserved_id(0, "must reserve an example identity");
}
