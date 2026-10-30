use crate::{BlobErrorCode, BlobResult, BlobState, add_seconds};
pub struct BlobRetention;
impl BlobRetention {
    pub fn unbound_eligible_at(completed_at: i64, seconds: u64) -> BlobResult<i64> {
        add_seconds(completed_at, seconds)
    }
    pub fn accepted_unreferenced_seconds(previous: u64, next: u64) -> u64 {
        previous.max(next)
    }
    pub fn unreferenced_eligible_at(
        unreferenced_at: i64,
        seconds: u64,
        floor: Option<i64>,
    ) -> BlobResult<i64> {
        let deadline = add_seconds(unreferenced_at, seconds)?;
        Ok(floor.map_or(deadline, |floor| floor.max(deadline)))
    }
    pub fn collectable(
        state: BlobState,
        binding_count: i64,
        now: i64,
        eligible_at: Option<i64>,
    ) -> bool {
        binding_count == 0
            && state != BlobState::Deleted
            && (state == BlobState::Deleting || eligible_at.is_some_and(|deadline| now >= deadline))
    }
    pub fn require_bindable(
        state: BlobState,
        now: i64,
        eligible_at: Option<i64>,
    ) -> BlobResult<()> {
        if state == BlobState::Initiated {
            return Err(BlobErrorCode::UploadIncomplete.into());
        }
        if !matches!(state, BlobState::Available | BlobState::UploadedUnbound)
            || eligible_at.is_some_and(|deadline| now >= deadline)
        {
            Err(BlobErrorCode::ReferenceExpired.into())
        } else {
            Ok(())
        }
    }
}
