use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::RuntimeError;

pub trait InternalIdGenerator: Send + Sync {
    fn generate_id(&self, entity: &str) -> Result<u64, RuntimeError>;
}

#[derive(Debug)]
pub struct SnowflakeIdGenerator {
    epoch_millis: u64,
    worker_id: u64,
    datacenter_id: u64,
    state: Mutex<SnowflakeState>,
}

#[derive(Debug, Default)]
struct SnowflakeState {
    last_timestamp: u64,
    sequence: u64,
}

impl Default for SnowflakeIdGenerator {
    fn default() -> Self {
        Self::new(0, 0)
    }
}

impl SnowflakeIdGenerator {
    const DEFAULT_EPOCH_MILLIS: u64 = 1_288_834_974_657;
    const WORKER_ID_BITS: u64 = 5;
    const DATACENTER_ID_BITS: u64 = 5;
    const SEQUENCE_BITS: u64 = 12;
    const MAX_WORKER_ID: u64 = (1 << Self::WORKER_ID_BITS) - 1;
    const MAX_DATACENTER_ID: u64 = (1 << Self::DATACENTER_ID_BITS) - 1;
    const SEQUENCE_MASK: u64 = (1 << Self::SEQUENCE_BITS) - 1;
    const WORKER_ID_SHIFT: u64 = Self::SEQUENCE_BITS;
    const DATACENTER_ID_SHIFT: u64 = Self::SEQUENCE_BITS + Self::WORKER_ID_BITS;
    const TIMESTAMP_SHIFT: u64 =
        Self::SEQUENCE_BITS + Self::WORKER_ID_BITS + Self::DATACENTER_ID_BITS;

    pub fn new(worker_id: u64, datacenter_id: u64) -> Self {
        assert!(worker_id <= Self::MAX_WORKER_ID, "worker id out of range");
        assert!(
            datacenter_id <= Self::MAX_DATACENTER_ID,
            "datacenter id out of range"
        );

        Self {
            epoch_millis: Self::DEFAULT_EPOCH_MILLIS,
            worker_id,
            datacenter_id,
            state: Mutex::new(SnowflakeState::default()),
        }
    }

    fn current_millis() -> Result<u64, RuntimeError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| RuntimeError::IdGeneration(err.to_string()))?;
        Ok(now.as_millis() as u64)
    }

    fn wait_until_next_millis(last_timestamp: u64) -> Result<u64, RuntimeError> {
        loop {
            let timestamp = Self::current_millis()?;
            if timestamp > last_timestamp {
                return Ok(timestamp);
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

impl InternalIdGenerator for SnowflakeIdGenerator {
    fn generate_id(&self, _entity: &str) -> Result<u64, RuntimeError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| RuntimeError::IdGeneration("snowflake state poisoned".to_owned()))?;
        let mut timestamp = Self::current_millis()?;

        if timestamp < state.last_timestamp {
            timestamp = Self::wait_until_next_millis(state.last_timestamp)?;
        }

        if timestamp == state.last_timestamp {
            state.sequence = (state.sequence + 1) & Self::SEQUENCE_MASK;
            if state.sequence == 0 {
                timestamp = Self::wait_until_next_millis(state.last_timestamp)?;
            }
        } else {
            state.sequence = 0;
        }

        state.last_timestamp = timestamp;

        let relative_timestamp = timestamp.checked_sub(self.epoch_millis).ok_or_else(|| {
            RuntimeError::IdGeneration("system clock is before snowflake epoch".to_owned())
        })?;

        Ok((relative_timestamp << Self::TIMESTAMP_SHIFT)
            | (self.datacenter_id << Self::DATACENTER_ID_SHIFT)
            | (self.worker_id << Self::WORKER_ID_SHIFT)
            | state.sequence)
    }
}

pub(crate) fn local_id_generator() -> &'static SnowflakeIdGenerator {
    static LOCAL_ID_GENERATOR: OnceLock<SnowflakeIdGenerator> = OnceLock::new();
    LOCAL_ID_GENERATOR.get_or_init(SnowflakeIdGenerator::default)
}
